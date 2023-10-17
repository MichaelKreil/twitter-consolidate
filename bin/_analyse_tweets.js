#!/bin/env node
"use strict"

import { each, pipeline, split, spy } from 'mississippi2';

let uncompressed_size = 0, tweet_count = 0, reply_count = 0, retweet_count = 0;

each(
	pipeline(
		process.stdin,
		spy(chunk => uncompressed_size += chunk.length),
		split()
	),
	(t, next) => {
		if (t.length < 3) return next();
		tweet_count++;
		t = JSON.parse(t);
		if (t.in_reply_to_status_id) reply_count++;
		if (t.retweeted_status) retweet_count++;
		next()
	},
	() => {
		process.stdout.write(JSON.stringify({
			uncompressed_size,
			tweet_count,
			reply_count,
			retweet_count
		}));
	}
);
