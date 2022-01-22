package schemaClasses;

import java.io.Serializable;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class PriceData implements Serializable {

	private static final long serialVersionUID = 1L;

	@SerializedName("close")
	@Expose
	private double close;
	@SerializedName("high")
	@Expose
	private double high;
	@SerializedName("low")
	@Expose
	private double low;
	@SerializedName("open")
	@Expose
	private double open;
	@SerializedName("volume")
	@Expose
	private double volume;

	private double profit;
	
	public double getOpen() {
		return open;
	}

	public void setOpen(double open) {
		this.open = open;
	}

	public double getHigh() {
		return high;
	}

	public void setHigh(double high) {
		this.high = high;
	}

	public double getLow() {
		return low;
	}

	public void setLow(double low) {
		this.low = low;
	}

	public double getClose() {
		return close;
	}

	public void setClose(double close) {
		this.close = close;
	}

	public double getVolume() {
		return volume;
	}

	public void setVolume(double volume) {
		this.volume = Math.abs(volume);
		this.profit=this.close-this.open;
	}

	public double getProfit() {
		return profit;
	}

	public void setProfit(double profit) {
		this.profit = profit;
	}
	
	@Override
	public String toString() {
		return "PriceData [open=" + open + ", high=" + high + ", low=" + low + ", close=" + close + ", volume=" + volume
				+ "]";
	}

}