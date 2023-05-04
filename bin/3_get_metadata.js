"use strict"

const {resolve,dirname} = require('path');
const {createRandomString} = require('../lib/helper.js');
const {createCommands} = require('../lib/cluster_helper.js');



const path = '/twitter/consolidated';
const tmpPath = '/tmp';
const maxDate = (new Date(Date.now()-3*86400000)).toISOString().slice(0,10);



createCommands('3_metadata', async sftp => {

	let todos = [];
	let directories = await sftp.getDirectories(path);
	for (let d of directories) {
		
		console.log(' - scan '+d.name);

		let files = await sftp.getFiles(d.fullname);
		files.forEach(f => {
			let match = f.fullname.match(/\/([^\/]*)_(\d\d\d\d-\d\d-\d\d)\.(jsonstream|tsv)\.xz$/);
			if (!match) return;

			let name = match[1];
			let date = match[2];
			let type = match[3];
			if (date > maxDate) return;
			
			let dstFullname = resolve(dirname(f.fullname), name+'_'+date+'.'+type+'.meta.txt');
			let tmpfile = resolve(tmpPath, 'result_'+createRandomString('3_'+dstFullname)+'.tmp');
			let order = [date, name, '3'].join('_');

			let commands = [
				'scp -P 22 -i ~/.ssh/box u227041@u227041.your-storagebox.de:'+f.fullname+' data.xz',
				'cat data.xz | pee \'wc -c | sed "s/^/compressed, number of bytes: /"\' \'openssl dgst -hex -md5 | sed -E "s/^(.* )?/compressed, hash, md5: /"\' \'openssl dgst -hex -sha1 | sed -E "s/^(.* )?/compressed, hash, sha1: /"\' \'openssl dgst -hex -sha256 | sed -E "s/^(.* )?/compressed, hash, sha256: /"\' \'openssl dgst -hex -sha512 | sed -E "s/^(.* )?/compressed, hash, sha512: /"\' > metadata_compressed.txt',
				'xz -dkc data.xz | pee \'wc -c | sed "s/^/uncompressed, number of bytes: /"\' \'wc -l | sed "s/^/uncompressed, number of lines: /"\' \'openssl dgst -hex -md5 | sed -E "s/^(.* )?/uncompressed, hash, md5: /"\' \'openssl dgst -hex -sha1 | sed -E "s/^(.* )?/uncompressed, hash, sha1: /"\' \'openssl dgst -hex -sha256 | sed -E "s/^(.* )?/uncompressed, hash, sha256: /"\' \'openssl dgst -hex -sha512 | sed -E "s/^(.* )?/uncompressed, hash, sha512: /"\' > metadata_uncompressed.txt',
				'echo "name: '+f.name+'" | cat - metadata_compressed.txt metadata_uncompressed.txt > metadata.txt',
				'scp -P 22 -i ~/.ssh/box metadata.txt u227041@u227041.your-storagebox.de:'+tmpfile,
				'echo "rename '+tmpfile+' '+dstFullname+'" | sftp -q -P 22 -i ~/.ssh/box u227041@u227041.your-storagebox.de 2>&1',
			];

			todos.push({dstFullname, order, commands});
		})
	}

	return todos;
})


