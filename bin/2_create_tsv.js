"use strict"

const {resolve} = require('path');
const {createRandomString} = require('../lib/helper.js');
const {createCommands} = require('../lib/cluster_helper.js');



const path = '/twitter/consolidated';
const tmpPath = '/tmp';
const maxDate = (new Date(Date.now()-3*86400000)).toISOString().slice(0,10);

let fields = [
	['id','.id_str'],
	['hashtags','try (.entities.hashtags | map(.text) | join(","))'],
	['urls','try (.entities.urls | map(.expanded_url) | join(","))'],
	['language','.metadata.iso_language_code'],
	['user_id','.user.id_str'],
	['user_name','.user.screen_name'],
	['retweeted_id','.retweeted_status.id_str'],
	['retweeted_user_id','.retweeted_status.user.id_str'],
	['retweeted_user_name','.retweeted_status.user.screen_name'],
];

let command = 'cat <(printf "'+fields.map(f => f[0]).join('\\t')+'\\n") <(xz -dck data.jsonstream.xz | jq -r \'['+fields.map(f => f[1]).join(',')+'] | @tsv\') | xz -z9e > data.tsv.xz';



createCommands('2_tsv', async sftp => {

	let todos = [];
	let directories = await sftp.getDirectories(path);
	for (let d of directories) {
		
		console.log(' - scan '+d.name);

		let files = await sftp.getFiles(d.fullname);
		files.forEach(f => {
			let match = f.fullname.match(/\/([^\/]*)_(\d\d\d\d-\d\d-\d\d)\.jsonstream\.xz$/);
			if (!match) return;

			let name = match[1];
			let date = match[2];
			if (date > maxDate) return;
			
			let dstFullname = f.fullname.replace('.jsonstream.xz', '.tsv.xz');
			let tmpfile = resolve(tmpPath, 'result_'+createRandomString('2_'+dstFullname)+'.tmp');
			let order = [date, name, '2'].join('_');

			let commands = [
				'scp -P 22 -i ~/.ssh/box u227041@u227041.your-storagebox.de:'+f.fullname+' data.jsonstream.xz',
				command,
				'scp -P 22 -i ~/.ssh/box data.tsv.xz u227041@u227041.your-storagebox.de:'+tmpfile,
				'echo "rename '+tmpfile+' '+dstFullname+'" | sftp -q -P 22 -i ~/.ssh/box u227041@u227041.your-storagebox.de 2>&1',
			];

			todos.push({dstFullname, order, commands});
		})
	}

	return todos;
})



