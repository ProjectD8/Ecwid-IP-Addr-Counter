package ecwid;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Optional;

public class IpAddrCounter
{
	public static class IpTreeNode
	{
		protected final int factor;
		
		protected IpTreeNode[] childNodes;
		protected BitSet leafSet;
		
		public IpTreeNode(int factor)
		{
			this.factor = factor;
		}
		
		public IpTreeNode getChildNode(int index)
		{
			if(childNodes == null)
			{
				childNodes = new IpTreeNode[factor];
			}
			
			if(childNodes[index] == null)
			{
				childNodes[index] = new IpTreeNode(factor);
			}
			
			return childNodes[index];
		}
		
		public void markLeaf(int index)
		{
			if(leafSet == null)
			{
				leafSet = new BitSet(factor);
			}
			
			leafSet.set(index);
		}
		
		public long cardinality()
		{
			long cardinality = 0;
			
			if(leafSet != null)
			{
				cardinality = leafSet.cardinality();
			}
			
			if(childNodes != null)
			{
				cardinality += Arrays.stream(childNodes)
					.parallel()
					.filter(child -> child != null)
					.map(child -> child.cardinality())
					.reduce(Long::sum)
					.orElse(0L);
			}
			
			return cardinality;
		}
	}
	
	public static void main(String[] args)
	{
		try
		{
			if(args.length >= 1)
			{
				countUniqueAddresses(args[0], Optional.of(System.out));
			}
			else
			{
				System.out.println("Usage: " + IpAddrCounter.class.getSimpleName() + " <file-with-addr-list>");
			}
		}
		catch(Throwable ex)
		{
			ex.printStackTrace();
		}
	}
	
	public static long countUniqueAddresses(String file, Optional<PrintStream> log) throws IOException
	{
		log.ifPresent(stream -> stream.println("Counting unique IP addresses from file \"" + file + "\""));
		
		try(InputStream input = new FileInputStream(file))
		{
			return countUniqueAddresses(input, log);
		}
	}
	
	public static long countUniqueAddresses(InputStream input, Optional<PrintStream> log) throws IOException
	{
		IpTreeNode rootNode = new IpTreeNode(256);
		
		long totalCount = 0;
		long errorCount = 0;
		
		long startTime = System.currentTimeMillis();
		
		try(BufferedReader reader = new BufferedReader(new InputStreamReader(input), 32 * 1024 * 1024))
		{
			String line;
			
			while((line = reader.readLine()) != null)
			{
				totalCount++;
				
				if(log.isPresent() && totalCount % 1000000 == 0)
				{
					log.get().println("..." + totalCount);
				}
				
				try
				{
					InetAddress inetAddress = InetAddress.getByName(line);
					
					if(inetAddress == null || !(inetAddress instanceof Inet4Address))
					{
						throw new UnknownHostException("not an IPv4 address: \"" + line + "\"");
					}
					
					byte[] addr = inetAddress.getAddress();
					
					IpTreeNode node = rootNode;
					
					for(int i = 0; i < addr.length - 1; i++)
					{
						node = node.getChildNode((int)addr[i] & 0xFF);
					}
					
					node.markLeaf((int)addr[addr.length - 1] & 0xFF);
				}
				catch(UnknownHostException ex)
				{
					errorCount++;
					log.ifPresent(stream -> stream.println("Warning: " + ex.getMessage()));
				}
			}
		}
		
		long fileReadTime = System.currentTimeMillis();
		
		long uniqueCount = rootNode.cardinality();
		
		long countingTime = System.currentTimeMillis();
		
		if(log.isPresent())
		{
			log.get().println("    Total addresses: " + totalCount);
			log.get().println("Erroneous addresses: " + errorCount);
			log.get().println("   Unique addresses: " + uniqueCount);
			
			log.get().println("File read time: " + (fileReadTime - startTime) + " ms");
			log.get().println(" Counting time: " + (countingTime - fileReadTime) + " ms");
			
			log.get().println("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " bytes");
		}
		
		return uniqueCount;
	}
}
