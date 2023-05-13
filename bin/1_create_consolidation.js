#!/bin/env node

"use strict"

import 'work-faster';
import { spawn } from 'child_process';
import { resolve } from 'path';
import { existsSync, mkdirSync, readdirSync, writeFileSync } from 'fs';

const topics = [
	// { name: 'corona', reg: /^corona/ },

	// { name: 'article13', reg: /^article13/ },
	// { name: 'brexit', reg: /^brexit/ },
	// { name: 'climate', reg: /^(climate|fridaysforfuture|gretathunberg)/ },
	// { name: 'epstein', reg: /^epstein/ },
	// { name: 'floridashooting', reg: /^floridashooting/ },
	// { name: 'hongkong', reg: /^hongkong/ },
	// { name: 'syria', reg: /^syria/ },
	{ name: 'racism', reg: /^racism/ },

	{ name: 'georgefloyd', reg: /^georgefloyd/ },
	{ name: 'trump', reg: /^trump/ },
	{ name: 'uselection2020', reg: /^uselection2020/ },
];

const srcPath = '/root/data/twitter/data_280';
const dstPath = '/root/data/twitter/consolidated';


for (let topic of topics) {
	await processTopic(topic);
}

async function processTopic(topic) {
	console.log(`Process topic "${topic.name}"`);

	let folderDst = resolve(dstPath, topic.name);
	mkdirSync(folderDst, { recursive: true });

	let folders = readdirSync(srcPath);
	folders = folders.filter(f => topic.reg.test(f));
	let entries = new Map();
	for (let folder of folders) {
		folder = resolve(srcPath, folder);
		let files = readdirSync(folder);
		files.forEach(file => {
			if (file.endsWith('.DAV')) return;
			let date = file.match(/_(\d{4}\-\d{2}\-\d{2})\.jsonstream\.xz$/);
			if (!date) throw Error(resolve(folder, file));
			date = date[1]
			if (!entries.has(date)) {
				entries.set(date, {
					date,
					files: [],
					order: date.split('').reverse().join(''),
					filename: resolve(folderDst, topic.name + '_' + date + '.jsonl.xz')
				})
			};
			entries.get(date).files.push(resolve(folder, file));
		})
	}
	entries = Array.from(entries.values());
	entries = entries.filter(e => !existsSync(e.filename));
	entries.sort((a, b) => a.order - b.order);

	let i = 0, n = entries.length;
	let progress = Progress();
	progress.update(0);

	await entries.forEachAsync(async entry => {
		let filenameDst = entry.filename;
		let filenameTmp = filenameDst + '.tmp';

		if (existsSync(filenameDst)) return;
		if (existsSync(filenameTmp)) return;
		writeFileSync(filenameTmp, '');

		let command = [
			'xz -dc', entry.files.join(' '),
			'| sort -ub --compress-program=lz4 | xz -z9e >', filenameTmp,
			'&& mv', filenameTmp, filenameDst
		].join(' ');

		await new Promise(res => {
			let child = spawn('bash', ['-c', command], { stdio: 'inherit' });
			child.on('error', (a, b, c) => console.log(a, b, c));
			child.on('close', code => {
				if (code !== 0) throw Error();
				res();
			});
		})

		i++;
		progress.update(i / n);
	})

	progress.finish();
}

function Progress(prefix = '   ') {
	let start = Date.now();

	return {
		update,
		finish,
	}
	function update(progress) {
		let eta = (Date.now() - start) * (1 - progress) / progress / 1000;
		let hours = Math.floor(eta / 3600);
		let minutes = Math.floor(eta / 60 - hours * 60);
		let seconds = Math.floor(eta - minutes * 60 - hours * 3600);
		eta = hours + ':' + ('00' + minutes).slice(-2) + ':' + ('00' + seconds).slice(-2);
		process.stderr.write('\x1b[2K\r' + prefix + (100 * progress).toFixed(2) + '% - ' + eta);
	}
	function finish() {
		process.stderr.write('\x1b[2K\r' + prefix + 'Finished\n');
	}
}
