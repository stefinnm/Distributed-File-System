import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;


public class Client
{
    DFS dfs;
    public Client(int p) throws Exception {
        dfs = new DFS(p);
        dfs.join(InetAddress.getLocalHost().toString(), p);
        //Results of ls
        System.out.println(dfs.ls());
        
        
        Scanner in = new Scanner(System.in);
        System.out.print("Enter file to rename: ");
        String oldName = in.nextLine();
        dfs.mv(oldName, "TestNewName");
        System.out.println(dfs.ls());
        
        System.out.print("Enter file to add: ");
        String newFile = in.nextLine();
        dfs.touch(newFile);
        System.out.println(dfs.ls());
        
        System.out.print("Enter file to delete: ");
        String deleteFile = in.nextLine();
        dfs.delete(deleteFile);
        System.out.println(dfs.ls());
        
            // User interface:
            // join, ls, touch, delete, read, tail, head, append, move
    }
    
    static public void main(String args[]) throws Exception
    {
    	
//        if (args.length < 1 ) {
//            throw new IllegalArgumentException("Parameter: <port>");
//        }
    	
//        Client client=new Client( Integer.parseInt(args[0]));
    	Client client=new Client(3003);
     } 
}
