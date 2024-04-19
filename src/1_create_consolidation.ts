#!/bin/env node
"use strict"

import { WF } from './lib/work-faster.ts';
import { spawn } from 'node:child_process';
import { resolve } from 'node:path';
import { mkdirSync, readdirSync } from 'node:fs';
import { Progress } from './lib/progress.ts';
import { access, stat } from 'node:fs/promises';

interface Topic {
	name: string,
	reg: RegExp,
}

interface Entry {
	date: string,
	files: string[],
	filename: string,
	size: number,
	ignore: boolean,
}

const topics: Topic[] = [
	{ name: 'corona', reg: /^corona/ },

	{ name: 'article13', reg: /^article13/ },
	{ name: 'brexit', reg: /^brexit/ },
	{ name: 'climate', reg: /^(climate|fridaysforfuture|gretathunberg)/ },
	{ name: 'epstein', reg: /^epstein/ },
	{ name: 'floridashooting', reg: /^floridashooting/ },
	{ name: 'hongkong', reg: /^hongkong/ },
	{ name: 'syria', reg: /^syria/ },
	{ name: 'racism', reg: /^racism/ },

	{ name: 'georgefloyd', reg: /^georgefloyd/ },
	{ name: 'trump', reg: /^trump/ },
	{ name: 'uselection2020', reg: /^uselection2020/ },

	{ name: 'brazilelection', reg: /^brazilelection/ },
	{ name: 'capitol', reg: /^capitol/ },
	{ name: 'infowars', reg: /^infowars/ },
	{ name: 'iranprotests', reg: /^iranprotests/ },
	{ name: 'ukraine', reg: /^ukraine/ },
];

const srcPath = '/root/data/twitter/data_280';
const dstPath = '/root/data/twitter/consolidated';


let allEntries: Entry[] = [];
for (let topic of topics) {
	console.log(`Process topic "${topic.name}"`);

	let folderDst = resolve(dstPath, topic.name);
	mkdirSync(folderDst, { recursive: true });

	const files = await getFiles(topic);
	const entries = await getEntries(topic, files, folderDst);
	allEntries.push(...entries);
}

await processEntries(allEntries);



async function getFiles(topic: Topic): Promise<string[]> {
	console.log('   scan folders')
	return readdirSync(srcPath)
		.filter(f => topic.reg.test(f))
		.map(folder => resolve(srcPath, folder))
		.flatMap(folder => {
			resolve(srcPath, folder);
			let files = readdirSync(folder);
			files = files.filter(file => !file.endsWith('.DAV'));
			return files.map(filename => resolve(folder, filename));
		});
}

async function getEntries(topic: Topic, files: string[], folderDst: string): Promise<Entry[]> {
	console.log(`   scan ${files.length} files`);
	let entries = new Map<string, Entry>();
	let i = 0;
	const n = files.length;
	const progress = Progress();
	await WF(files).forEachAsync(async filenameIn => {
		i++;
		progress.update(i / n);

		const { size } = await stat(filenameIn);
		if (size < 64) return;

		let dateMatch = filenameIn.match(/_(\d{4}\-\d{2}\-\d{2})\.jsonstream\.xz$/);
		if (!dateMatch) throw Error(filenameIn);
		const date = dateMatch[1]

		if (!entries.has(date)) {
			const filenameOut = resolve(folderDst, topic.name + '_' + date + '.jsonl.xz');
			let ignore = true;
			try {
				await access(filenameOut)
			} catch (e) {
				ignore = false;
			}
			entries.set(date, {
				date,
				files: [],
				filename: filenameOut,
				ignore,
				size: 0,
			})
		};

		const entry = entries.get(date) as Entry;
		entry.files.push(filenameIn);
		entry.size += size;
	})
	progress.finish();

	const entryList = Array.from(entries.values()).filter(e => !e.ignore);

	return entryList;
}

async function processEntries(entries: Entry[]) {
	console.log(`   process ${entries.length} entries`);

	let i = 0;
	const n = entries.reduce((s, e) => s + e.size, 0);
	const progress = Progress();
	progress.update(0);

	await WF(entries).forEachAsync(async entry => {
		let filenameDst = entry.filename;
		let filenameTmp = filenameDst + '.tmp';

		let command = [
			'xz -dc', entry.files.join(' '),
			'| sort -ub --compress-program=gzip | xz -z9e >', filenameTmp,
			'&& mv', filenameTmp, filenameDst
		].join(' ');

		await new Promise(res => {
			let child = spawn('bash', ['-c', command], { stdio: 'inherit' });
			child.on('error', (a: any, b: any, c: any) => console.log(a, b, c));
			child.on('close', code => {
				if (code !== 0) {
					console.error('error for ' + filenameDst);
				};
				res(null);
			});
		})

		i += entry.size;
		progress.update(i / n);
	})

	progress.finish();
}
