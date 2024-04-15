#!/bin/env node
"use strict"

import 'work-faster';
import { spawn } from 'child_process';
import { resolve } from 'path';
import { existsSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'fs';
import { Progress } from './lib.js';


const srcPath = '/root/data/twitter/consolidated';
const tmpPath = '/root/data/twitter/consolidated_cache';

const files = [];

console.log('scan:');
readdirSync(srcPath).forEach(topic => {
	let path = resolve(srcPath, topic);
	if (!statSync(path).isDirectory()) return;
	console.log('   - ' + topic);
	readdirSync(path).forEach(filename => {
		if (!filename.endsWith('.jsonl.xz')) return;
		let fullname = resolve(path, filename);
		let size = statSync(fullname).size;
		files.push({ topic, filename, fullname, size });
	})
})

files.sort(() => Math.random() - 0.5);

let sizePos = 0;
let sizeSum = files.reduce((s, f) => s + f.size, 0);
console.log(`process ${files.length} files with ${(sizeSum / 0x40000000).toFixed(1)} GB:`);
let progress = Progress();
await files.forEachAsync(async file => {
	const cachedFile = cacheFile(file);
	file.content = await analyseContent(cachedFile);
	file.hash = await analyseHash(cachedFile);
	cachedFile.free()

	sizePos += file.size;
	progress.update(sizePos / sizeSum);
}, 2)

progress.finish();

console.log('save data');

const topics = new Map();
files.forEach(file => {
	let topic = topics.get(file.topic);
	if (!topic) {
		topic = [];
		topics.set(file.topic, topic);
	}
	topic.push({
		filename: file.filename,
		size_compressed: file.size,
		size_uncompressed: file.content.uncompressed_size,
		tweet_count: file.content.tweet_count,
		reply_count: file.content.reply_count,
		retweet_count: file.content.retweet_count,
		sha256: file.hash.sha256,
		md5: file.hash.md5,
	})
})

Array.from(topics.entries()).forEach(([topic, files]) => {
	files.sort((a, b) => a.filename < b.filename ? -1 : 1);
	const filename = resolve(srcPath, 'filelist-' + topic);
	writeFileSync(filename + '.json', JSON.stringify(files));
	writeFileSync(filename + '.csv', csv(files));
})

function csv(data) {
	const keys = Array.from(Object.keys(data[0]));
	data = data.map(entry => keys.map(k => entry[k]));
	data.unshift(keys);
	return data.map(entry => entry.join(',') + '\n').join('');
}

async function analyseContent(file) {
	let tempFilename = file.getTempName('content');

	if (existsSync(tempFilename)) {
		return JSON.parse(readFileSync(tempFilename));
	}

	let filename = await file.cacheLocally();
	const data = await new Promise(res => {
		const child = spawn(
			'bash',
			['-c', `cat "${filename}" | xz -d | node _analyse_tweets.js`]
		);
		const buffers = [];
		child.stdout.on('data', chunk => buffers.push(chunk));
		child.stderr.on('data', chunk => console.log(String(chunk)));
		child.on('error', (...error) => {
			console.log({ error, filename });
			process.exit();
		});
		child.on('close', code => {
			if (code !== 0) {
				console.log({ filename });
				throw Error();
			}
			res(JSON.parse(Buffer.concat(buffers)));
		});
	})
	writeFileSync(tempFilename, JSON.stringify(data));
	return data;
}

async function analyseHash(file) {
	let tempFilename = file.getTempName('hash');

	if (existsSync(tempFilename)) {
		return JSON.parse(readFileSync(tempFilename));
	}

	let filename = await file.cacheLocally();
	const data = {
		sha256: await calcHash('sha256sum'),
		md5: await calcHash('md5sum'),
	}
	writeFileSync(tempFilename, JSON.stringify(data));
	return data;

	function calcHash(command) {
		return new Promise(res => {
			const child = spawn(command, [filename]);
			const buffers = [];
			child.stdout.on('data', chunk => buffers.push(chunk));
			child.stderr.on('data', chunk => console.log(String(chunk)));
			child.on('error', (...error) => {
				console.log({ error, filename });
				process.exit();
			});
			child.on('close', code => {
				if (code !== 0) {
					console.log({ filename });
					throw Error();
				}
				res(Buffer.concat(buffers).toString().replace(/\s.*/s, ''));
			});
		})
	}
}

function cacheFile(file) {
	let cachedFilename;
	return { cacheLocally, free, getTempName }
	function getTempName(category) {
		return resolve(tmpPath, category, file.filename.replace(/\.json.*/, '.json'));
	}
	async function cacheLocally() {
		cachedFilename = resolve('/root/data/temp/', file.filename);
		await new Promise(res => {
			const child = spawn('cp', [file.fullname, cachedFilename], { stdio: 'inherit' });
			child.on('close', code => {
				if (code !== 0) throw Error();
				res();
			});
		})
		return cachedFilename;
	}
	function free() {
		if (cachedFilename) rmSync(cachedFilename);
	}
}
