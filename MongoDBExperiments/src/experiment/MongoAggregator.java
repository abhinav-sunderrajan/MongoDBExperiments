package experiment;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import bean.TrafficBean;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;


/**
 * 
 * A very simple test which uses the pre aggregation design pattern.
 *
 */
public class MongoAggregator {

	private Mongo connection;
	private WriteConcern concern;
	private DB db;
	private DBCollection collection;
	private DBUpsertor upsert;
	private static MongoAggregator agg;
	private class DBUpsertor implements Runnable{
		
		Random random=new Random();

		@Override
		public void run() {
			
	while(true){
			for(int count=100;count<400;count++){
					updateCollection(new TrafficBean(new Date(),count,random.nextFloat()*100,random.nextInt(100)));
			}
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
	}
		
	}
	
	
	public MongoAggregator() {
		try {
			connection=new Mongo("localhost", 27017);
			concern=new WriteConcern(1,2000);
			//Drop data base
			db=connection.getDB("Traffic");
			collection=db.getCollection("Aggregates");
			new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			upsert=new DBUpsertor();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main (String []args){
		agg=new MongoAggregator();	
		agg.createStructure();
		Thread upsert=new Thread(agg.upsert);
		upsert.setName("Mongo Aggregator");
		upsert.start();
		
	}
	
	private void createStructure(){
		DBObject aggColums=new BasicDBObject();
		aggColums.put("count",0);
		aggColums.put("totalSpeed",0);
		aggColums.put("totalVolume",0);
		for(int linkId=100;linkId<400;linkId++){
			DBObject doc = new BasicDBObject();
			doc.put("_id", linkId);
			doc.put("total", aggColums);
			DBObject hours=new BasicDBObject();
			for(int hour=0;hour<24;hour++){
				hours.put(Integer.toString(hour), aggColums);
			}
			
			doc.put("hourly", hours);
			DBObject minutes=new BasicDBObject();
			for(int min=0;min<60;min++){
				minutes.put(Integer.toString(min), aggColums);
			}
			
			DBObject minAgg=new BasicDBObject();
			for(int hour=0;hour<24;hour++){
				minAgg.put(Integer.toString(hour), minutes);
			}
			
			doc.put("minutes", minAgg);
			collection.insert(doc,concern);
			
		}
		
	}
	
	private void updateCollection(TrafficBean bean){
		DBObject query = new BasicDBObject("_id",bean.getLinkId());
		DBObject inc= new BasicDBObject();
		inc.put("total.count", 1);
		inc.put("hourly."+bean.getDate().getHours()+".count",1);
		inc.put("hourly."+bean.getDate().getHours()+".totalSpeed", bean.getSpeed());
		inc.put("hourly."+bean.getDate().getHours()+".totalVolume", bean.getVolume());
		inc.put("minutes."+bean.getDate().getHours()+"."+bean.getDate().getMinutes()+".count",1);
		inc.put("minutes."+bean.getDate().getHours()+"."+bean.getDate().getMinutes()+".totalSpeed", bean.getSpeed());
		inc.put("minutes."+bean.getDate().getHours()+"."+bean.getDate().getMinutes()+".totalVolume", bean.getVolume());
		
		DBObject obj=new BasicDBObject().append("$inc", inc);
		collection.update(query,obj,true,false);
	}
}
