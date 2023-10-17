#!/bin/env node
"use strict"

import 'work-faster';
import { spawn } from 'child_process';
import { resolve } from 'path';
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from 'fs';
import { Progress } from './lib.js';


const srcPath = '/root/data/twitter/consolidated';
const tmpPath = '/root/data/twitter/tmp';
mkdirSync(tmpPath, { recursive: true });

const files = [];

console.log('scan:');
readdirSync(srcPath).forEach(topic => {
	let path = resolve(srcPath, topic);
	if (!statSync(path).isDirectory) return;
	console.log('   - ' + topic);
	readdirSync(path).forEach(filename => {
		if (!filename.endsWith('.jsonl.xz')) return;
		files.push({ topic, filename, fullname: resolve(path, filename) });
	})
})

files.sort(() => Math.random() - 0.5);

console.log(`process ${files.length} files:`);
let progress = Progress(), index = 0;
progress.update(0);
await files.forEachAsync(async (file, i) => {
	let tempFilename = resolve(tmpPath, file.filename.replace(/\.json.*/, '.json'));

	if (existsSync(tempFilename)) {
		file.data = JSON.parse(readFileSync(tempFilename));
		index++;
		progress.update(index / files.length);
		return
	}

	file.data = await new Promise(res => {
		const child = spawn(
			'bash',
			['-c', `cat "${file.fullname}" | xz -d | node _analyse_tweets.js`]
		);
		const buffers = [];
		child.stdout.on('data', chunk => buffers.push(chunk));
		child.on('error', (a, b, c) => console.log(a, b, c));
		child.on('close', code => {
			if (code !== 0) throw Error();
			res(JSON.parse(Buffer.concat(buffers)));
		});
	})

	writeFileSync(tempFilename, JSON.stringify(file.data));
	index++;
	progress.update(index / files.length);
}, 8)

progress.finish();
