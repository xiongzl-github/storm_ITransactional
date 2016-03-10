package com.gbdex;

import java.math.BigInteger;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Values;

public class MyEmitter implements ITransactionalSpout.Emitter<MyMata> {

	Map<Long, String> dbMap = null;

	public MyEmitter(Map<Long, String> dbMap) {
		this.dbMap = dbMap;
	}

	//在关闭发射器之前可以做的事情.
	@Override
	public void cleanupBefore(BigInteger txid) {
	}

	//释放这个发射器所有的资源.
	@Override
	public void close() {
	}

	//发射一个Batch
	@Override
	public void emitBatch(TransactionAttempt tx, MyMata coordinatorMeta, BatchOutputCollector collector) {
		long beginPoint = coordinatorMeta.getBeginPoint();
		int num = coordinatorMeta.getNum();

		for (long i = beginPoint; i < num + beginPoint; i++) {
			if (dbMap.get(i) == null) {
				continue;
			}
			collector.emit(new Values(tx, dbMap.get(i)));
			
			System.err.println("ThreadId: "+Thread.currentThread().getId()+"----MyTransactionBolt.emitBatch: TransactionId: " + tx.getTransactionId() + "  attemptid: "
					+ tx.getAttemptId());
		}
	}

}
