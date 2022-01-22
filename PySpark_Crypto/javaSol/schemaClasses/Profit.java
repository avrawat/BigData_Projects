package schemaClasses;

import java.io.Serializable;

public class Profit implements Serializable {

	private static final long serialVersionUID = 1L;

	private double cumprofit;
	private double cumloss;
	private int countRDDs;

	public Profit() {
		this.cumprofit = 0.0;
		this.cumloss = 0.0;
		this.countRDDs = 0;
	}

	public double getCumprofit() {
		return cumprofit;
	}

	public void setCumprofit(double cumprofit) {
		this.cumprofit = cumprofit;
	}

	public double getCumloss() {
		return cumloss;
	}

	public void setCumloss(double cumloss) {
		this.cumloss = cumloss;
	}

	public int getCountRDDs() {
		return countRDDs;
	}

	public void setCountRDDs(int countRDDs) {
		this.countRDDs = countRDDs;
	}

}