package com.flink.iot.basics.entities

import org.apache.flink.api.common.time.Time

case class CacheEntry[T](timestamp: Long, cached: T) {

	def isExpired(timeout: Time): Boolean = {
		(System.currentTimeMillis() - timeout.toMilliseconds) >= timestamp
	}
}
