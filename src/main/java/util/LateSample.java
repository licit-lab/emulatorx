package util;

import java.time.LocalDateTime;

public class LateSample {
	private LocalDateTime receivedDateTime;
	private float sampleSpeed;
	private float coverage;

	public LateSample(LocalDateTime receivedDateTime, float sampleSpeed, float coverage){
		this.receivedDateTime = receivedDateTime;
		this.sampleSpeed = sampleSpeed;
		this.coverage = coverage;
	}

	public LocalDateTime getReceivedDateTime() {
		return receivedDateTime;
	}

	public void setReceivedDateTime(LocalDateTime receivedDateTime) {
		this.receivedDateTime = receivedDateTime;
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
