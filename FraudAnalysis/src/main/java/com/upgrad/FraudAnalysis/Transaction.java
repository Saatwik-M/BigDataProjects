package com.upgrad.FraudAnalysis;

import java.io.Serializable;

public class Transaction implements Serializable{
	
	
	private static final long serialVersionUID = 1L;
	private long card_id;
	private long member_id;
	private long amount;
	private long pos_id;
	private long postcode;
	private String transaction_dt;
	private int member_score;
	private double ucl;
	private String status;
	
	public long getCard_id() {
		return card_id;
	}
	public void setCard_id(long card_id) {
		this.card_id = card_id;
	}
	public long getMember_id() {
		return member_id;
	}
	public void setMember_id(long member_id) {
		this.member_id = member_id;
	}
	public long getAmount() {
		return amount;
	}
	public void setAmount(long amount) {
		this.amount = amount;
	}
	public long getPos_id() {
		return pos_id;
	}
	public void setPos_id(long pos_id) {
		this.pos_id = pos_id;
	}
	public long getPostcode() {
		return postcode;
	}
	public void setPostcode(long postcode) {
		this.postcode = postcode;
	}
	public String getTransaction_dt() {
		return transaction_dt;
	}
	public void setTransaction_dt(String transaction_dt) {
		this.transaction_dt = transaction_dt;
	}
	
	public int getMember_score() {
		return member_score;
	}
	public void setMember_score(int member_score) {
		this.member_score = member_score;
	}
	public double getUcl() {
		return ucl;
	}
	public void setUcl(double ucl) {
		this.ucl = ucl;
	}
	
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	@Override
	public String toString() {
		return "Transaction [card_id=" + card_id + ", member_id=" + member_id + ", amount=" + amount + ", pos_id="
				+ pos_id + ", postcode=" + postcode + ", transaction_dt=" + transaction_dt + ", member_score="
				+ member_score + ", ucl=" + ucl + ", status=" + status + "]";
	}
	/**
	 * 
	 */
}
