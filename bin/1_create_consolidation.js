#!/bin/env node

"use strict"

import 'work-faster';
import { spawn } from 'child_process';
import { resolve } from 'path';
import { existsSync, mkdirSync, readdirSync } from 'fs';
// import { createRandomString } from '../lib/helper.js';
// import { createCommands } from '../lib/cluster_helper.js';

const [mod, rem] = [[2, 1], [3, 0]].map(([i, d]) => {
	let v = parseInt(process.argv[i], 10);
	return isNaN(v) ? d : v
});


const topics = [
	//{name:'brexit', reg:/^brexit/},
	//{name:'climate', reg:/^(climate|fridaysforfuture|gretathunberg)/},
	{ name: 'corona', reg: /^corona/ },
	//{name:'epstein', reg:/^epstein/},
	//{name:'georgefloyd', reg:/^georgefloyd/},
	//{name:'media', reg:/^media/},
	//{name:'trump', reg:/^trump/},
	//{name:'uselection2020', reg:/^uselection2020/},
];

const srcPath = '/root/data/twitter/data_280';
const dstPath = '/root/data/twitter/consolidated';
const tmpPath = '/root/data/tmp';


for (let topic of topics) {
	await processTopic(topic);
}

async function processTopic(topic) {
	console.log(`Process topic "${topic.name}"`);

	let folders = readdirSync(srcPath);
	folders = folders.filter(f => topic.reg.test(f));
	let entries = new Map();
	for (let folder of folders) {
		folder = resolve(srcPath, folder);
		let files = readdirSync(folder);
		files.forEach(file => {
			if (file.endsWith('.DAV')) return;
			//if (!file.endsWith('.jsonstream.xz')) return;
			let date = file.match(/_(\d{4}\-\d{2}\-\d{2})\.jsonstream\.xz$/)[1];
			if (!entries.has(date)) entries.set(date, { date, files: [] });
			entries.get(date).files.push(resolve(folder, file));
		})
	}
	entries = Array.from(entries.values());
	entries = entries.filter(e => Math.round(Date.parse(e.date) / 86400000) % mod === rem);
	entries.sort((a, b) => a.date < b.date ? -1 : 1);

	let folderDst = resolve(dstPath, topic.name);
	mkdirSync(folderDst, { recursive: true });

	await entries.forEachAsync(async entry => {
		console.log('start', entry.date);

		let filenameDst = resolve(folderDst, topic.name + '_' + entry.date + '.jsonl.xz');
		let filenameTmp = resolve(folderDst, topic.name + '_' + entry.date + '.tmp.xz');

		if (existsSync(filenameDst)) return;
		let command = [
			'xz -dc', entry.files.join(' '),
			'| sort -ub --compress-program=lz4 | xz -z9e >', filenameTmp,
			'&& mv', filenameTmp, filenameDst
		].join(' ');

		await new Promise(res => {
			let child = spawn('bash', ['-c', command]);
			child.on('close', code => {
				//console.log(`child process exited with code ${code}`);
				res();
			});
		})
	})
}
