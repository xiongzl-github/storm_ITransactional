package com.gbdex;

import java.math.BigInteger;

import backtype.storm.transactional.ITransactionalSpout;
import backtype.storm.utils.Utils;

public class MyCoordinator implements ITransactionalSpout.Coordinator<MyMata>{

	public static int BATCH_NUM = 10 ;
	@Override
	public void close() {
	}

	//初始化事务
	@Override
	public MyMata initializeTransaction(BigInteger txid, MyMata prevMetadata) {
		long beginPoint = 0;
		if (prevMetadata == null) {
			beginPoint = 0 ;
		}else {
			beginPoint = prevMetadata.getBeginPoint() + prevMetadata.getNum() ;
		}
		MyMata mata = new MyMata() ;
		mata.setBeginPoint(beginPoint);
		mata.setNum(BATCH_NUM);
		System.err.println("启动一个事务："+mata.toString());
		return mata;
	}

	/**
	 * 返回true则开启一个事务, 否则忽略这个事务.
	 */
	@Override
	public boolean isReady() {
		Utils.sleep(2000);
		return true;
	}

}
