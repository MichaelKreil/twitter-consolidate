
const { each, pipeline, split2, spy } = require('mississippi2');
const { createReadStream } = require('node:fs');

let size = 0, count = 0, types = new Map();
const filename = process.argv[2];
each(
	pipeline(
		createReadStream(filename),
		spy(chunk => size += chunk.length),
		split2()
	),
	(t, next) => {
		if (t.length < 3) return next();
		count++;
		t = JSON.parse(t);
		let type = [];
		if (t.in_reply_to_status_id) type.push('reply');
		if (t.retweeted_status) type.push('retweet');
		if (t.is_quote_status) type.push('quote');
		type = type.join(',');
		types.set(type, (types.get(type) || 0) + 1);
		next()
	},
	() => {
		console.log({ size, count, types });
	}
);

