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
		let fullname = resolve(path, filename);
		let size = statSync(fullname).size;
		files.push({ topic, filename, fullname, size });
	})
})

files.sort(() => Math.random() - 0.5);

let progress = Progress();
let sizePos = 0;
let sizeSum = files.reduce((s, f) => s + f.size, 0);
progress.update(0);
console.log(`process ${files.length} files with ${(sizeSum / 0x40000000).toFixed(1)} GB:`);
await files.forEachAsync(async (file, i) => {
	let tempFilename = resolve(tmpPath, file.filename.replace(/\.json.*/, '.json'));

	if (existsSync(tempFilename)) {
		file.data = JSON.parse(readFileSync(tempFilename));
	} else {
		file.data = await new Promise(res => {
			const child = spawn(
				'bash',
				['-c', `cat "${file.fullname}" | xz -d | node _analyse_tweets.js`]
			);
			const buffers = [];
			child.stdout.on('data', chunk => buffers.push(chunk));
			child.stderr.on('data', chunk => console.log(String(chunk)));
			child.on('error', (...args) => {
				console.log(args);
				console.log({ file });
				process.exit();
			});
			child.on('close', code => {
				if (code !== 0) throw Error();
				res(JSON.parse(Buffer.concat(buffers)));
			});
		})
		writeFileSync(tempFilename, JSON.stringify(file.data));
	}

	sizePos += file.size;
	progress.update(sizePos / sizeSum);
}, 8)

progress.finish();

console.log(files);
