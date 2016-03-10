package com.gbdex;

import java.util.Map;
import java.util.Random;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MyTransactionBolt extends BaseTransactionalBolt {
	private static final long serialVersionUID = 1L;
	Integer count = 0;
	BatchOutputCollector collector;
	TransactionAttempt tx;

	@Override
	public void execute(Tuple tuple) {
		tx = (TransactionAttempt) tuple.getValue(0);
		System.err.println("ThreadId: "+Thread.currentThread().getId()+"----MyTransactionBolt.execute: TransactionId: " + tx.getTransactionId() + "  attemptid: "
				+ tx.getAttemptId());
		String log = tuple.getString(1);
		if (log != null && log.length() > 0) {
			count++;
		}
		
		int randomNum = new Random().nextInt(10) + 1;
		if (randomNum == 5) {
			try {
				throw new Exception();
			} catch (Exception e) {
				System.err.println("ERROR: ==================>>ThreadId: "+Thread.currentThread().getId()+"----MyTransactionBolt.execute: TransactionId: " + tx.getTransactionId() + "  attemptid: "
						+ tx.getAttemptId());
				e.printStackTrace();
			}
		}
	}

	@Override
	public void finishBatch() {
		System.err.println("finishBatch " + count);
		collector.emit(new Values(tx, count));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt tx) {
		this.collector = collector;

		System.err.println("ThreadId: "+Thread.currentThread().getId()+"----MyTransactionBolt.execute: TransactionId: " + tx.getTransactionId() + "  attemptid: "
				+ tx.getAttemptId());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx", "count"));
	}

}
