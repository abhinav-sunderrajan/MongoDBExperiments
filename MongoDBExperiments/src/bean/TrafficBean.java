package bean;

import java.util.Date;

public class TrafficBean {
	
	private Date date;
	private int linkId;
	private float speed;
	private int volume;
	
	public TrafficBean(Date date,int linkId,float speed,int volume){
		this.date=date;
		this.linkId=linkId;
		this.speed=speed;
		this.volume=volume;
		
	}
	public Date getDate() {
		return date;
	}
	
	public int getLinkId() {
		return linkId;
	}
	
	public float getSpeed() {
		return speed;
	}
	
	public int getVolume() {
		return volume;
	}
	

}
