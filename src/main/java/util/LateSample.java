package util;

import java.time.LocalDateTime;

public class LateSample {
	private LocalDateTime receivedDate;
	private float sampleSpeed;
	private float coverage;

	public LateSample(LocalDateTime receivedDate, float sampleSpeed, float coverage){
		this.receivedDate = receivedDate;
		this.sampleSpeed = sampleSpeed;
		this.coverage = coverage;
	}

	public LocalDateTime getReceivedDate() {
		return receivedDate;
	}

	public void setReceivedDate(LocalDateTime receivedDate) {
		this.receivedDate = receivedDate;
	}

	public float getSampleSpeed() {
		return sampleSpeed;
	}

	public void setSampleSpeed(float sampleSpeed) {
		this.sampleSpeed = sampleSpeed;
	}

	public float getCoverage() {
		return coverage;
	}

	public void setCoverage(float coverage) {
		this.coverage = coverage;
	}
}
