
export function Progress(prefix = '   ') {
	let start = Date.now();

	return {
		update,
		finish,
	}
	function update(progress:number) {
		const eta = (Date.now() - start) * (1 - progress) / progress / 1000;
		const hours = Math.floor(eta / 3600);
		const minutes = Math.floor(eta / 60 - hours * 60);
		const seconds = Math.floor(eta - minutes * 60 - hours * 3600);
		const etaString = hours + ':' + ('00' + minutes).slice(-2) + ':' + ('00' + seconds).slice(-2);
		process.stderr.write('\x1b[2K\r' + prefix + (100 * progress).toFixed(2) + '% - ' + etaString);
	}
	function finish() {
		//process.stderr.write('\x1b[2K\r' + prefix + 'Finished\n');
		process.stderr.write('\x1b[2K\r');
	}
}
