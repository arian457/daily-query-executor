package com.arian.dqe.contracts;

public enum ExecutionStatus {
	RETRY, DONE, FAILED_PERMANENT, FAILED_EXHAUSTED;

	public boolean isFinal() {
		return this != RETRY;
	}
}
