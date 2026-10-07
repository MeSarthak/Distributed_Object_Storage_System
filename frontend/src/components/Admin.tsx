import React, { useState, useEffect } from 'react';
import { Terminal, Activity, Server, Cpu, Database } from 'lucide-react';
import api from '../api';

export default function Admin() {
  const [nodes, setNodes] = useState([]);
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [nodesRes, logsRes] = await Promise.all([
          api.get('/cluster/nodes'),
          api.get('/logs')
        ]);
        setNodes(nodesRes.data.data.nodes || []);
        setLogs(logsRes.data.data.logs || []);
      } catch (err) {
        console.error('Failed to fetch admin data', err);
      }
    };
    fetchData();
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-8">
      <div>
        <h1 className="text-3xl font-semibold text-white tracking-tight">Cluster Admin</h1>
        <p className="text-gray-400 mt-1">Hardware metrics and system audit logs</p>
      </div>

      <h2 className="text-xl font-semibold text-white">Storage Nodes</h2>
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {nodes.map((node: any) => (
          <div key={node.node_id} className="bg-obsidian-light p-6 rounded-2xl border border-gray-800">
            <div className="flex justify-between items-start mb-6">
              <div className="flex items-center gap-3">
                <div className={`p-2 rounded-lg ${node.status === 'ONLINE' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400'}`}>
                  <Server size={24} />
                </div>
                <div>
                  <h3 className="font-medium text-white">{node.hostname}</h3>
                  <p className="text-sm text-gray-500">{(node.node_id || '').substring(0,8)}</p>
                </div>
              </div>
              <span className={`text-xs px-2 py-1 rounded-full border ${node.status === 'ONLINE' ? 'border-emerald-500/30 text-emerald-400' : 'border-red-500/30 text-red-400'}`}>
                {node.status}
              </span>
            </div>
            
            <div className="space-y-4">
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-400 flex items-center gap-1"><Cpu size={14}/> CPU</span>
                  <span className="text-gray-300">{node.cpu_usage?.toFixed(1) || 0}%</span>
                </div>
                <div className="w-full bg-obsidian rounded-full h-1.5"><div className="bg-primary h-full rounded-full" style={{width: `${node.cpu_usage || 0}%`}}></div></div>
              </div>
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-400 flex items-center gap-1"><Activity size={14}/> RAM</span>
                  <span className="text-gray-300">{node.memory_usage?.toFixed(1) || 0}%</span>
                </div>
                <div className="w-full bg-obsidian rounded-full h-1.5"><div className="bg-purple-500 h-full rounded-full" style={{width: `${node.memory_usage || 0}%`}}></div></div>
              </div>
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-400 flex items-center gap-1"><Database size={14}/> Storage</span>
                  <span className="text-gray-300">
                    {((node.used_storage || 0) / (1024*1024)).toFixed(0)}MB / {((node.total_storage || 1) / (1024*1024)).toFixed(0)}MB
                  </span>
                </div>
                <div className="w-full bg-obsidian rounded-full h-1.5"><div className="bg-amber-500 h-full rounded-full" style={{width: `${((node.used_storage || 0)/(node.total_storage || 1))*100}%`}}></div></div>
              </div>
            </div>
          </div>
        ))}
        {nodes.length === 0 && <div className="text-gray-500 col-span-3">No nodes connected to cluster.</div>}
      </div>

      <h2 className="text-xl font-semibold text-white pt-4">System Audit Logs</h2>
      <div className="bg-[#0a0e17] rounded-2xl border border-gray-800 overflow-hidden flex flex-col h-96">
        <div className="bg-gray-900 px-4 py-3 border-b border-gray-800 flex items-center gap-2">
          <Terminal size={16} className="text-gray-500" />
          <span className="text-sm text-gray-400 font-mono">/var/log/syslog</span>
        </div>
        <div className="p-4 font-mono text-sm overflow-y-auto flex-1 space-y-1">
          {logs.map((log: any) => (
            <div key={log.log_id} className="flex gap-4">
              <span className="text-gray-600 w-48 shrink-0">{new Date(log.timestamp).toISOString()}</span>
              <span className={`w-16 shrink-0 ${log.severity === 'ERROR' ? 'text-red-400' : log.severity === 'WARN' ? 'text-amber-400' : 'text-emerald-400'}`}>[{log.severity}]</span>
              <span className="text-blue-300 w-24 shrink-0">{log.event_type}</span>
              <span className="text-gray-300">{log.description}</span>
            </div>
          ))}
          {logs.length === 0 && <div className="text-gray-600">Waiting for logs...</div>}
        </div>
      </div>
    </div>
  );
}
