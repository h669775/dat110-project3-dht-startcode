package no.hvl.dat110.middleware;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

public class MutualExclusion {
        
    private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
    private boolean CS_BUSY = false;						
    private boolean WANTS_TO_ENTER_CS = false;				
    private List<Message> queueack; 						
    private List<Message> mutexqueue;						
    private LamportClock clock;								
    private Node node;
    
    public MutualExclusion(Node node) throws RemoteException {
        this.node = node;
        clock = new LamportClock();
        queueack = new ArrayList<Message>();
        mutexqueue = new ArrayList<Message>();
    }
    
    public synchronized void acquireLock() {
        CS_BUSY = true;
        logger.info(node.getNodeName() + " has acquired the lock.");
    }
    
    public void releaseLocks() {
        WANTS_TO_ENTER_CS = false;
        CS_BUSY = false;
        logger.info(node.getNodeName() + " has released the locks.");
    }

    public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
		logger.info(node.nodename + " wants to access CS");
		
		// Clear queues and set flags
		queueack.clear();
		mutexqueue.clear();
		
		// Update clock and message
		clock.increment();
		message.setClock(clock.getClock());
		WANTS_TO_ENTER_CS = true;
	
		// Get all active nodes for voting and remove self
		List<Message> activenodes = removeDuplicatePeersBeforeVoting();
		activenodes.removeIf(m -> m.getNodeName().equals(node.getNodeName()));
	
		// Add self to mutexqueue first
		mutexqueue.add(message);
	
		// Multicast request to all other active nodes
		multicastMessage(message, activenodes);
	
		// Wait for all acknowledgments 
		int expectedAcks = activenodes.size(); // NOT including self since we're not sending message to self
		int waited = 0;
		int maxWait = 10000;
		
		while (queueack.size() < expectedAcks && waited < maxWait) {
			try {
				Thread.sleep(10);
				waited += 10;
			} catch (InterruptedException e) {
				break;
			}
		}
		
		// If we got all acknowledgments
		if (queueack.size() == expectedAcks) {
			// Find the message with lowest clock value or lowest node ID if clock values are equal
			Message lowestMsg = message;
			for (Message m : mutexqueue) {
				if (m.getClock() < lowestMsg.getClock() || 
				   (m.getClock() == lowestMsg.getClock() && 
					m.getNodeID().compareTo(lowestMsg.getNodeID()) < 0)) {
					lowestMsg = m;
				}
			}
			
			// Check if we have the lowest value
			if (lowestMsg.getNodeName().equals(message.getNodeName())) {
				acquireLock();
				node.broadcastUpdatetoPeers(updates);
				return true;
			}
		}
		
		WANTS_TO_ENTER_CS = false;
		return false;
	}
    
    private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
		logger.info("Number of peers to vote = " + activenodes.size());
		for (Message peer : activenodes) {
			NodeInterface stub = Util.getProcessStub(peer.getNodeName(), peer.getPort());
			if (stub != null) {
				// Create a fresh message for each peer
				Message msg = new Message(message.getNodeID(), message.getNodeName(), message.getPort());
				msg.setClock(message.getClock());
				stub.onMutexRequestReceived(msg);
			}
		}
	}
    
    public void onMutexRequestReceived(Message message) throws RemoteException {
        clock.increment();
        int caseid = -1;
        if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
            caseid = 0;
        } else if (CS_BUSY) {
            caseid = 1;
        } else if (WANTS_TO_ENTER_CS) {
            caseid = 2;
        }
        doDecisionAlgorithm(message, mutexqueue, caseid);
    }
    
    public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
		String procName = message.getNodeName();
		int port = message.getPort();
		
		switch (condition) {
			case 0: { // Not accessing or wanting to access CS
				NodeInterface stub = Util.getProcessStub(procName, port);
				if (stub != null) {
					message.setAcknowledged(true);
					stub.onMutexAcknowledgementReceived(message);
				}
				queue.add(message);  // Add message to queue
				break;
			}
			case 1: { // Currently in CS
				queue.add(message);
				break;
			}
			case 2: { // Wants to access CS
				int senderClock = message.getClock();
				int ownClock = clock.getClock();
				
				if (senderClock < ownClock || 
				   (senderClock == ownClock && 
					message.getNodeID().compareTo(node.getNodeID()) < 0)) {
					NodeInterface stub = Util.getProcessStub(procName, port);
					if (stub != null) {
						message.setAcknowledged(true);
						stub.onMutexAcknowledgementReceived(message);
					}
				}
				queue.add(message);  // Always add message to queue
				break;
			}
			default:
				break;
		}
	}
    
    public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
        queueack.add(message);
    }
    
    public void multicastReleaseLocks(Set<Message> activenodes) {
        logger.info("Releasing locks from = " + activenodes.size());
        for (Message peer : activenodes) {
            NodeInterface stub = Util.getProcessStub(peer.getNodeName(), peer.getPort());
            if (stub != null) {
                try {
                    stub.releaseLocks();
                } catch (RemoteException e) {
                    logger.error("Error releasing locks for peer: " + peer.getNodeName());
                }
            }
        }
    }
    
    private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
        logger.info(node.getNodeName() + ": size of queueack = " + queueack.size());
        if (queueack.size() == numvoters) {
            queueack.clear();
            return true;
        }
        return false;
    }
    
    private List<Message> removeDuplicatePeersBeforeVoting() {
        List<Message> uniquepeer = new ArrayList<Message>();
        for(Message p : node.activenodesforfile) {
            boolean found = false;
            for(Message p1 : uniquepeer) {
                if(p.getNodeName().equals(p1.getNodeName())) {
                    found = true;
                    break;
                }
            }
            if(!found)
                uniquepeer.add(p);
        }		
        return uniquepeer;
    }
}