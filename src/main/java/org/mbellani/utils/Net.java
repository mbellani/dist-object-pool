package org.mbellani.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class Net {

	private static String pickAddressFromInterfaces() {
		String pick = "unknown";
		try {
			Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
			while (nics.hasMoreElements()) {
				NetworkInterface nic = nics.nextElement();
				Enumeration<InetAddress> adrs = nic.getInetAddresses();
				while (adrs.hasMoreElements()) {
					InetAddress adr = adrs.nextElement();
					if (adr instanceof Inet4Address && adr.isSiteLocalAddress()) {
						pick = adr.getHostAddress();
					}
				}
			}
			return pick;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static String getAddress() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException ex) {
			return pickAddressFromInterfaces();
		}
	}

}
