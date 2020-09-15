package goZzzWorker

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// RedisConn is redis connection struct
type RedisConn struct {
	conn *redis.Client
}

// NewRedisConn create redis connection
func NewRedisConn(address string, password string, db int) *RedisConn {
	redisConn := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})
	return &RedisConn{
		conn: redisConn,
	}
}

// GetZRangeByScoreLessThan get ZSet values whose the score is less than number
func (rc *RedisConn) GetZRangeByScoreLessThan(key string, number string) (valueArray []string, retErr error) {
	ctx := context.Background()
	zRangeByScore := rc.conn.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: number})
	if err := zRangeByScore.Err(); err != nil {
		retErr = err
		return
	}
	valueArray = zRangeByScore.Val()
	return
}

// RemoveZSet remove values from ZSet
func (rc *RedisConn) RemoveZSet(key string, valueArray []string) (retErr error) {
	ctx := context.Background()
	zRem := rc.conn.ZRem(ctx, key, valueArray)
	if err := zRem.Err(); err != nil {
		retErr = err
	}
	return
}

// GetHashValue get hash value
func (rc *RedisConn) GetHashValue(key string, field string) (value string, retErr error) {
	ctx := context.Background()
	hGet := rc.conn.HGet(ctx, key, field)
	if err := hGet.Err(); err != nil {
		retErr = err
		return
	}
	value = hGet.Val()
	return
}

// RemoveHash remove hash
func (rc *RedisConn) RemoveHash(key string, field string) (retErr error) {
	ctx := context.Background()
	hDel := rc.conn.HDel(ctx, key, field)
	if err := hDel.Err(); err != nil {
		retErr = err
	}
	return
}
