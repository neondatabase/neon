import React, { useState, useEffect } from 'react';
import ReactDOM from 'react-dom';
import Loader from "react-loader-spinner";

function walpos_to_int(walpos)
{
    const [hi, lo] = walpos.split('/');

    return parseInt(hi, 16) + parseInt(lo, 16);
}

const palette = [
    "#003f5c",
    "#2f4b7c",
    "#665191",
    "#a05195",
    "#d45087",
    "#f95d6a",
    "#ff7c43",
    "#ffa600"];

function WalRecord(props)
{
    const firstwalpos = props.firstwalpos;
    const endwalpos = props.endwalpos;
    const record = props.record;
    const index = props.index;
    const xidmap = props.xidmap;

    const startpos = walpos_to_int(record.start)
    const endpos = walpos_to_int(record.end)

    const scale = 1000 / (16*1024*1024)
    const startx = (startpos - firstwalpos) * scale;
    const endx = (endpos - firstwalpos) * scale;

    const xidindex = xidmap[record.xid];
    const color = palette[index % palette.length];

    const y = 5 + (xidindex) * 20 + (index % 2) * 2;
    
    return (
	<line x1={ startx } y1={y} x2={endx} y2={y} stroke={ color } strokeWidth="5">
	    <title>
		start: { record.start } end: { record.end }
	    </title>
	</line>
    )
}

function WalFile(props)
{
    const walContent = props.walContent;
    const firstwalpos = props.firstwalpos;
    const xidmap = props.xidmap;
   
    return <svg width="1000" height="200">
	       {
		   walContent.records ? 
 		       walContent.records.map((record, index) =>
			   <WalRecord key={record.start} firstwalpos={firstwalpos} record={record} index={index} xidmap={xidmap}/>
		       ) : "no records"
	       }
	   </svg>
}

function WalDumpApp()
{
    const [walContent, setWalContent] = useState({});

    const filename = '00000001000000000000000C';

    useEffect(() => {
	fetch('/fetch_wal?filename='+filename).then(res => res.json()).then(data => {
	    setWalContent(data);
	});
    }, []);

    var firstwalpos = 0;
    var endwalpos = 0;
    var numxids = 0;
    var xidmap = {};
    if (walContent.records && walContent.records.length > 0)
    {
	firstwalpos = walpos_to_int(walContent.records[0].start);
	endwalpos = firstwalpos + 16*1024*1024;

	walContent.records.forEach(rec => {
	    if (!xidmap[rec.xid])
	    {
		xidmap[rec.xid] = ++numxids;
	    }
	});
    }

    return (
	<>
	    <h2>{filename}</h2>
	    <WalFile walContent={walContent} firstwalpos={firstwalpos} endwalpos={endwalpos} xidmap={xidmap}/>
	</>
    );
}

console.log('hey there');
ReactDOM.render(<WalDumpApp/>, document.getElementById('waldump'));
