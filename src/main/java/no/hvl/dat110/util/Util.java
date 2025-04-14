package no.hvl.dat110.util;


/**
 * dat110
 * @author tdoy
 */

import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import no.hvl.dat110.middleware.Node;
import no.hvl.dat110.rpc.interfaces.NodeInterface;

public class Util {
	 
	public static String activeIP = null;
	public static int numReplicas = 4;  
	
	/**
	 * This method computes (lower <= id <= upper).
	 * To use this method to compute (lower < id <= upper), ensure that the calling method increased the lower param by 1.
	 * To use this method to compute (lower <= id < upper), ensure that the calling method increased the upper param by 1.
	 * To use this method to compute (lower < id < upper), ensure that the calling method increased both the lower and upper params by 1.
	 * @param id
	 * @param lower
	 * @param upper
	 * @return true if (lower <= id <= upper) or false otherwise
	 */
	public static boolean checkInterval(BigInteger id, BigInteger lower, BigInteger upper) {
		System.out.println("Checking interval: id=" + id + ", lower=" + lower + ", upper=" + upper);
	
		if (lower.compareTo(upper) < 0) {
			// Case 1: Normal interval (e.g., 6 <= id <= 9)
			boolean result = id.compareTo(lower) >= 0 && id.compareTo(upper) <= 0;
			System.out.println("Case 1 (Normal interval): Result = " + result);
			return result;
		} else if (lower.compareTo(upper) > 0) {
			// Case 2: Wrapped interval (e.g., 9 < id <= 2 in mod space)
			boolean result = id.compareTo(lower) > 0 || id.compareTo(upper) <= 0;
			System.out.println("Case 2 (Wrapped interval): Result = " + result);
			return result;
		} else {
			// Case 3: lower == upper (entire address space)
			System.out.println("Case 3 (Entire address space): Result = true");
			return true;
		}
	}
	
	public static List<String> toString(List<NodeInterface> list) throws RemoteException {
		List<String> nodestr = new ArrayList<String>();
		list.forEach(node -> 
			{
				nodestr.add(((Node)node).getNodeName());
			}
		);
		
		return nodestr;
	}
	
	public static NodeInterface getProcessStub(String name, int port) {
		
		NodeInterface nodestub = null;
		Registry registry = null;
		try {
			// Get the registry for this worker node
			registry = LocateRegistry.getRegistry(port);		
			
			nodestub = (NodeInterface) registry.lookup(name);	// remote stub
			
		} catch (NotBoundException | RemoteException e) {
			return null;			// if this call fails, then treat the node to have left the ring...or unavailable
		}
		
		return nodestub;
	}
	
	/**
	 * This method is used when processes are running on a single computer
	 * @return the registry for the found ip
	 * @throws RemoteException 
	 * @throws NumberFormatException 
	 */
	public static Registry tryIPSingleMachine(String nodeip) throws NumberFormatException, RemoteException {
		
		// try the tracker IP addresses and connect to any one available
		String[] ips = StaticTracker.ACTIVENODES;
		List<String> iplist = Arrays.asList(ips);
		Collections.shuffle(iplist);
		
		Registry registry = null;
		for (String ip : iplist) {
			String ipaddress = ip.split(":")[0].trim();
			String port = ip.split(":")[1].trim();
			System.out.println(ipaddress+":"+port);
			if(nodeip.equals(ipaddress))
				continue;
			registry = LocateRegistry.getRegistry(Integer.valueOf(port));
			if (registry != null) {
				activeIP = ipaddress;
				return registry;
			}
		}
		
		return registry;

	}
	
	public static Map<String, Integer> getProcesses(){
		
		Map<String, Integer> processes = new HashMap<>();
		processes.put("process1", 9091);
		processes.put("process2", 9092);
		processes.put("process3", 9093);
		processes.put("process4", 9094);
		processes.put("process5", 9095);
		
		return processes;
	}

}
