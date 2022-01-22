package schemaClasses;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class StockPrice implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	@SerializedName("priceData")
	@Expose
	private PriceData priceData;
	@SerializedName("symbol")
	@Expose
	private String symbol;
	@SerializedName("timestamp")
	@Expose
	private String timestamp;
	
	public PriceData getPriceData() {
		return priceData;
	}

	public void setPriceData(PriceData priceData) {
		this.priceData = priceData;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	
	public String getTimestamp() {
		return this.timestamp;
	}
	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	
	/*
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd hh:mm:ss")
	public Date getTimestamp() throws ParseException {
		return formatter.parse(this.timestamp);
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = formatter.format(timestamp);
	}
*/
	@Override
	public String toString() {
		return "StockPrice [symbol=" + symbol + ", timestamp=" + timestamp + ", priceData=" + priceData + "]";
	}

}