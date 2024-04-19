
export function Progress(prefix = '   ') {
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
