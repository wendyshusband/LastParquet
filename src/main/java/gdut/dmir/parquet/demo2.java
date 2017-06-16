package gdut.dmir.parquet;

import test.Message;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class demo2 {
    public static LinkedList<Message> s(String fileName,double speed,int length)throws IOException {

        LinkedList<Message> nodelist = new LinkedList<Message>();

        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(fileName)));

        String line;
        while ((line = br.readLine()) != null) {
            int i;
            for (i = 0; i < line.length(); i++) if (line.charAt(i) == '|') break;
            String Key = line.substring(0, i);
            long l = Long.parseLong(Key);
            nodelist.add(new Message(l, line));
        }
        return nodelist;
    }

	public static void contralSpeed(String fileName,double speed,int length)throws IOException{

    	List<Node> nodelist = new LinkedList<Node>();
        
        BufferedReader br = new BufferedReader(
				new InputStreamReader(
						new FileInputStream(fileName)));

        String line;
		while((line = br.readLine())!=null){
			int i;
		    for(i=0;i<line.length();i++)  if(line.charAt(i)=='|')  break;
		    String Key=line.substring(0,i);
		    long l=Long.parseLong(Key);
		    nodelist.add(new Node(l,line));
		}

		int cnt=(int)(speed*1024*1024*1.0/length);
		double t=1000.0/cnt;
		int block=1;
		while(t<10.0){
			t*=10;
			block*=10;
		}
		int f=(int)(t+0.5);
		
		long start=0;
		String V;
		int num=0,mark=0;
		int total=0;
		
		long Start=System.currentTimeMillis();
		for (Node node : nodelist) {
			 
        	if(num==0&&mark==1){
        		long js=System.currentTimeMillis();
        		long Stop=f-(js-start);
        		
        		if(Stop>0){
        			try {
    					Thread.sleep(Stop);
    				} catch (InterruptedException e) {
    					e.printStackTrace();
    				}
        		}
        		mark=0;
        	}
        	if(num==0&&mark==0){
         		start=System.currentTimeMillis();
        		mark=1;
        		num=block;
        	}
        	
        	V=node.getV();
        	num--;
        	total++;
		}
		long End=System.currentTimeMillis();
		//System.out.println("?????"+100000.0/(End-Start));
	
		br.close();
    }

}
