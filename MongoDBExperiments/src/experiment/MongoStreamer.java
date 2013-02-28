package experiment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.BSONObject;

import server.WebSocketBridge;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * 
 * Retrieve records from MongoDB and stream it to the web socket bridge.
 *
 */

public class MongoStreamer {

	private Mongo connection;
	private DB db;
	private DBCollection collection;
	private DBRetriever retreive;
	private DBStreamer stream;
	private static MongoStreamer streamer;
	private ConcurrentLinkedQueue<DBObject> queue;
	private WebSocketBridge bridge;
	
	
	private class DBStreamer implements Runnable{
		DBObject obj;
		DBObject analyze;
		
		//Start jetty server to send data to browser
		DBStreamer(){
			bridge.startServer();
		}
		
		@Override
		public void run() {
			
			while(true){
				if(!queue.isEmpty()){
					break;
				}
				
			}
			while(!queue.isEmpty()){
				
				try {
					obj=queue.poll();
					analyze=(DBObject) ((DBObject) obj.get("hourly")).get("21");
					System.out.println("Link Id "+obj.get("linkId")+" stats: Average speed = "+(Double)analyze.get("totalSpeed")/(Integer)analyze.get("count")
							+"Average volume = "+(Integer)analyze.get("totalVolume")/(Integer)analyze.get("count"));
					bridge.sendMessage(analyze.toString());
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		}
		
	}
	
	
	
	private class DBRetriever implements Runnable{		

		@Override
		public void run() {
			DBObject filter = new BasicDBObject();
			DBObject projection= new BasicDBObject();
			DBObject range=new BasicDBObject();
			range.put("$gte", 100);
			range.put("$lte", 399);
			filter.put("_id", range);
			Date now=new Date();
				projection.put("hourly."+(/*now.getHours()*/21)+
						".count", 1);
				projection.put("hourly."+(/*now.getHours()*/21)+
						".totalSpeed", 1);
				projection.put("hourly."+(/*now.getHours()*/21)+
						".totalVolume", 1);
				DBCursor cursor =collection.find(filter, projection);
				System.out.println(cursor.count());
				while(cursor.hasNext()){
					queue.add(cursor.next());
				}
				
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	
	public MongoStreamer() {
		try {
			connection=new Mongo("localhost", 27017);
			//Drop data base
			db=connection.getDB("Traffic");
			collection=db.getCollection("Aggregates");
			queue=new ConcurrentLinkedQueue<DBObject>();
			bridge=new WebSocketBridge();
			
			//Instantiate the threads
			retreive=new DBRetriever();
			stream=new DBStreamer();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main (String []args){
		streamer=new MongoStreamer();	
		Thread retrieve=new Thread(streamer.retreive);
		retrieve.setName("Producer");
		Thread stream=new Thread(streamer.stream);
		stream.setName("Consumer");
		retrieve.start();
		stream.start();
	}
	
	private byte[] encode(BSONObject obj) throws IOException{
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
	    ObjectOutputStream out = new ObjectOutputStream(byteOut);
	    out.writeObject(obj);
	    return byteOut.toByteArray();
	}

}
