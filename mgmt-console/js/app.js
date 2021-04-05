import React, { useState, useEffect } from 'react';
import ReactDOM from 'react-dom';
import Loader from "react-loader-spinner";
import { Router, Route, Link, IndexRoute, hashHistory, browserHistory } from 'react-router';

function ServerStatus(props) {
    const datadir = props.server.datadir;
    const status = props.server.status;
    const port = props.server.port;

    return (
	<div>
	    <h2>{ datadir == 'primary' ? 'Primary' : datadir }</h2>
	    status: <div className='status'>{status}</div><br/>
	    to connect: <span className='shellcommand'>psql -h { window.location.hostname } -p { port } -U zenith postgres</span><br/>
	</div>
    );
}

function StandbyList(props) {
    const bucketSummary = props.bucketSummary;
    const standbys = props.standbys;
    const maxwalpos = bucketSummary.maxwal ? walpos_to_int(bucketSummary.maxwal) : 0;

    const [walposInput, setWalposInput] = useState({ src: 'text', value: '0/0'});

    // find earliest base image
    const minwalpos = bucketSummary.nonrelimages ? bucketSummary.nonrelimages.reduce((minpos, imgpos_str, index, array) => {
	const imgpos = walpos_to_int(imgpos_str);
	return (minpos == 0 || imgpos < minpos) ? imgpos : minpos;
    }, 0) : 0;

    const can_create_standby = minwalpos > 0 && maxwalpos > 0 && maxwalpos >= minwalpos;
    var walpos_valid = true;

    function create_standby() {
	const formdata = new FormData();
	formdata.append("walpos", walposStr);

	props.startOperation('Creating new standby at ' + walposStr + '...',
			     fetch("/api/create_standby", { method: 'POST', body: formdata }));
    }

    function destroy_standby(datadir) {
	const formdata = new FormData();
	formdata.append("datadir", datadir);
	props.startOperation('Destroying ' + datadir + '...',
			     fetch("/api/destroy_server", { method: 'POST', body: formdata }));
    }

    const handleSliderChange = (event) => {
	setWalposInput({ src: 'slider', value: event.target.value });
    }    

    const handleWalposChange = (event) => {
	setWalposInput({ src: 'text', value: event.target.value });
    }

    var sliderValue;
    var walposStr;
    if (walposInput.src == 'text')
    {
	const walpos = walpos_to_int(walposInput.value);

	if (walpos >= minwalpos && walpos <= maxwalpos)
	    walpos_valid = true;
	else
	    walpos_valid = false;
	
	sliderValue = Math.round((walpos - minwalpos) / (maxwalpos - minwalpos) * 100);
	walposStr = walposInput.value;
    }
    else
    {
	const slider = walposInput.value;
	const new_walpos = minwalpos + slider / 100 * (maxwalpos - minwalpos);

	console.log('minwalpos: '+ minwalpos);
	console.log('maxwalpos: '+ maxwalpos);

	walposStr = int_to_walpos(Math.round(new_walpos));
	walpos_valid = true;
	console.log(walposStr);
    }

    var standbystatus = ''
    if (standbys)
    {
	standbystatus = 
	    <div>
		{
		    standbys.length > 0 ? 
 			standbys.map((server) =>
			    <>
				<ServerStatus key={ 'status_' + server.datadir} server={server}/>
				<button key={ 'destroy_' + server.datadir} onClick={e => destroy_standby(server.datadir)}>Destroy standby</button>
			    </>
			) : "no standby servers"
		}
	    </div>
    }

    return (
	<div>
	    <h2>Standbys</h2>
	    <button onClick={create_standby} disabled={!can_create_standby || !walpos_valid}>Create new Standby</button> at LSN 
            <input type="text" id="walpos_input" value={ walposStr } onChange={handleWalposChange} disabled={!can_create_standby}/>
	    <input type="range" id="walpos_slider" min="0" max="100" steps="1" value={sliderValue}  onChange={handleSliderChange} disabled={!can_create_standby}/>
	    <br/>
	    { standbystatus }
	</div>
    );
}

function ServerList(props) {
    const primary = props.serverStatus ? props.serverStatus.primary : null;
    const standbys = props.serverStatus ? props.serverStatus.standbys : [];
    const bucketSummary = props.bucketSummary;

    var primarystatus = '';

    function destroy_primary() {
	const formdata = new FormData();
	formdata.append("datadir", 'primary');
	props.startOperation('Destroying primary...',
			     fetch("/api/destroy_server", { method: 'POST', body: formdata }));
    }    

    function restore_primary() {
	props.startOperation('Restoring primary...',
			     fetch("/api/restore_primary", { method: 'POST' }));
    }    
    
    if (primary)
    {
	primarystatus =
	    <div>
		<ServerStatus server={primary}/>
		<button onClick={destroy_primary}>Destroy primary</button>
	    </div>
    }
    else
    {
	primarystatus =
	    <div>
		no primary server<br/>
		<button onClick={restore_primary}>Restore primary</button>
	    </div>
    }

    return (
	<>
	    { primarystatus }
	    <StandbyList standbys={standbys} startOperation={props.startOperation} bucketSummary={props.bucketSummary}/>
	    <p className="todo">
		Should we list the WAL safekeeper nodes here? Or are they part of the Storage? Or not visible to users at all?
	    </p>
	</>
    );
}

function BucketSummary(props) {
    const bucketSummary = props.bucketSummary;
    const startOperation = props.startOperation;

    function slicedice() {
	startOperation('Slicing sequential WAL to per-relation WAL...',
		       fetch("/api/slicedice", { method: 'POST' }));
    }
    
    if (!bucketSummary.nonrelimages)
    {
	return <>loading...</>
    }

    return (
	<div>
	    <div>Base images at following WAL positions:
		<ul>
		    {bucketSummary.nonrelimages.map((img) => (
			<li key={img}>{img}</li>
		    ))}
		</ul>
	    </div>
            Sliced WAL is available up to { bucketSummary.maxwal }<br/>
	    Raw WAL is available up to { bucketSummary.maxseqwal }<br/>

	    <br/>
	    <button onClick={slicedice}>Slice & Dice WAL</button>
	    <p className="todo">
		Currently, the slicing or "sharding" of the WAL needs to be triggered manually, by clicking the above button.
		<br/>
		TODO: make it a continuous process that runs in the WAL safekeepers, or in the Page Servers, or as a standalone service.
	    </p>
	</div>
    );
}

function ProgressIndicator()
{
    return (
	<div>
	    <Loader
		type="Puff"
		color="#00BFFF"
		height={100}
		width={100}
	    />
	</div>
    )
}

function walpos_to_int(walpos)
{
    const [hi, lo] = walpos.split('/');

    return parseInt(hi, 16) + parseInt(lo, 16);
}

function int_to_walpos(x)
{
    console.log('converting ' + x);
    return (Math.floor((x / 0x100000000)).toString(16) + '/' + (x % 0x100000000).toString(16)).toUpperCase();
}

function OperationStatus(props) {
    const lastOperation = props.lastOperation;
    const inProgress = props.inProgress;
    const operationResult = props.operationResult;

    if (lastOperation)
    {
	return (
	    <div><h2>Last operation:</h2>
		<div>{lastOperation} { (!inProgress && lastOperation) ? 'done!' : '' }</div>
		<div className='result'>
		    {inProgress ? <ProgressIndicator/> : <pre>{operationResult}</pre>}
		</div>
	    </div>
	);
    }
    else
	return '';
}

function ActionButtons(props) {

    const startOperation = props.startOperation;
    const bucketSummary = props.bucketSummary;
    
    function reset_demo() {
	startOperation('resetting everything...',
		       fetch("/api/reset_demo", { method: 'POST' }));
    }

    function init_primary() {
	startOperation('Initializing new primary...',
		       fetch("/api/init_primary", { method: 'POST' }));
    }

    function zenith_push() {
	startOperation('Pushing new base image...',
		       fetch("/api/zenith_push", { method: 'POST' }));
    }
	
    return (
	<div>
	    <p className="todo">
		RESET DEMO deletes everything in the storage bucket, and stops and destroys all servers. This resets the whole demo environment to the initial state.
	    </p>
	    <button onClick={reset_demo}>RESET DEMO</button>
	    <p className="todo">
		Init Primary runs initdb to create a new primary server. Click this after Resetting the demo.
	    </p>

	    <button onClick={init_primary}>Init primary</button>

	    <p className="todo">
		Push Base Image stops the primary, copies the current state of the primary to the storage bucket as a new base backup, and restarts the primary.
		<br/>
		TODO: This should be handled by a continuous background process, probably running in the storage nodes. And without having to shut down the cluster, of course.
	    </p>

	    <button onClick={zenith_push}>Push base image</button>

	</div>
    );
}

function Sidenav(props)
{
    const toPage = (page) => (event) => {
	//event.preventDefault()
	props.switchPage(page);
    };
    return (
	<div>
	    <h3 className="sidenav-item">Menu</h3>
	    <a href="#servers" onClick={toPage('servers')} className="sidenav-item">Servers</a>
	    <a href="#storage" onClick={toPage('storage')} className="sidenav-item">Storage</a>
	    <a href="#snapshots" onClick={toPage('snapshots')} className="sidenav-item">Snapshots</a>
	    <a href="#demo" onClick={toPage('demo')} className="sidenav-item">Demo</a>
	    <a href="#import" onClick={toPage('import')}  className="sidenav-item">Import / Export</a>
	    <a href="#jobs" onClick={toPage('jobs')} className="sidenav-item">Jobs</a>
	</div>
    );
}

function App()
{
    const [page, setPage] = useState('servers');
    const [serverStatus, setServerStatus] = useState({});
    const [bucketSummary, setBucketSummary] = useState({});
    const [lastOperation, setLastOperation] = useState('');
    const [inProgress, setInProgress] = useState('');
    const [operationResult, setOperationResult] = useState('');

    useEffect(() => {
	reloadStatus();
    }, []);

    function startOperation(operation, promise)
    {
	promise.then(result => result.text()).then(resultText => {
	    operationFinished(resultText);
	});
	
	setLastOperation(operation);
	setInProgress(true);
	setOperationResult('');
    }

    function operationFinished(result)
    {
	setInProgress(false);
	setOperationResult(result);
	reloadStatus();
    }

    function clearOperation()
    {
	setLastOperation('')
	setInProgress('');
	setOperationResult('');
	console.log("cleared");
    }
    
    function reloadStatus()
    {
	fetch('/api/server_status').then(res => res.json()).then(data => {
	    setServerStatus(data);
	});

	fetch('/api/bucket_summary').then(res => res.json()).then(data => {
	    setBucketSummary(data);
	});
    }

    const content = () => {
	console.log(page);
	if (page === 'servers') {
	    return (
		<>
		    <h1>Server status</h1>
		    <ServerList startOperation={ startOperation }
				serverStatus={ serverStatus }
				bucketSummary={ bucketSummary }/>
		</>
	    );
	} else if (page === 'storage') {
	    return (
		<>
		    <h1>Storage bucket status</h1>
		    <BucketSummary startOperation={ startOperation }
				   bucketSummary={ bucketSummary }/>
		</>
	    );
	} else if (page === 'snapshots') {
	    return (
		<>
		    <h1>Snapshots</h1>
		    <p className="todo">
			In Zenith, snapshots are just specific points (LSNs) in the WAL history, with a label. A snapshot prevents garbage collecting old data that's still needed to reconstruct the database at that LSN.
		    </p>
		    <p className="todo">
			TODO:
			<ul>
			    <li>List existing snapshots</li>
			    <li>Create new snapshot manually, from current state or from a given LSN</li>
			    <li>Drill into the WAL stream to see what have happened. Provide tools for e.g. finding point where a table was dropped</li>
			    <li>Create snapshots automatically based on events in the WAL, like if you call pg_create_restore_point(() in the primary</li>
			    <li>Launch new reader instance at a snapshot</li>
			    <li>Export snapshot</li>
			    <li>Rollback cluster to a snapshot</li>
			</ul>
		    </p>
		</>
	    );
	} else if (page === 'demo') {
	    return (
		<>
		    <h1>Misc actions</h1>
		    <ActionButtons startOperation={ startOperation }
				   bucketSummary={ bucketSummary }/>
		</>
	    );
	} else if (page === 'import') {
	    return (
		<>
		    <h1>Import & Export tools</h1>
		    <p className="TODO">TODO:
			<ul>
			    <li>Initialize database from existing backup (pg_basebackup, WAL-G, pgbackrest)</li>
			    <li>Initialize from a pg_dump or other SQL script</li>
			    <li>Launch batch job to import data files from S3</li>
			    <li>Launch batch job to export database with pg_dump to S3</li>
			</ul>
			These jobs can be run in against reader processing nodes. We can even
			spawn a new reader node dedicated to a job, and destry it when the job is done.
		    </p>
		</>
	    );
	} else if (page === 'jobs') {
	    return (
		<>
		    <h1>Batch jobs</h1>
		    <p className="TODO">TODO:
			<ul>
			    <li>List running jobs launched from Import & Export tools</li>
			    <li>List other batch jobs launched by the user</li>
			    <li>Launch new batch jobs</li>
			</ul>
		    </p>
		</>
	    );
	}
    }

    function switchPage(page)
    {
	console.log("topage " + page);
	setPage(page)
	clearOperation();
    };

    return (
	<div className="row">
	    <div className="sidenav">
		<Sidenav switchPage={switchPage} className="column"/>
	    </div>
	    <div className="column">
		<div>
		    { content() }
		</div>
		<OperationStatus lastOperation={ lastOperation }
				 inProgress = { inProgress }
				 operationResult = { operationResult }/>
	    </div>
	</div>
    );
}

ReactDOM.render(<App/>, document.getElementById('reactApp'));
