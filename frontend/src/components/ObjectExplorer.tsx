import React, { useState, useEffect } from 'react';
import { Search, Download, Trash2, Copy, FileText, Check } from 'lucide-react';
import api from '../api';

export default function ObjectExplorer() {
  const [objects, setObjects] = useState([]);
  const [search, setSearch] = useState('');
  const [tierFilter, setTierFilter] = useState('ALL');
  const [copiedId, setCopiedId] = useState('');

  useEffect(() => {
    fetchObjects();
  }, []);

  const fetchObjects = async () => {
    try {
      const res = await api.get('/objects');
      const objectsWithTier = (res.data.data.objects || []).map((o: any) => ({
        ...o,
        tier: o.replication_factor >= 5 ? 'HOT' : o.replication_factor <= 2 ? 'COLD' : 'WARM'
      }));
      setObjects(objectsWithTier);
    } catch (err) {
      console.error('Failed to fetch objects', err);
    }
  };

  const handleDelete = async (id: string) => {
    if (!confirm('Are you sure you want to cascade purge this object?')) return;
    try {
      await api.delete(`/objects/${id}`);
      fetchObjects();
    } catch (err) {
      alert('Failed to delete object');
    }
  };

  const handleDownload = async (id: string, name: string) => {
    try {
      const res = await api.get(`/objects/${id}`, { responseType: 'blob' });
      const url = window.URL.createObjectURL(res.data);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', name || 'downloaded_file');
      document.body.appendChild(link);
      link.click();
      link.parentNode?.removeChild(link);
      window.URL.revokeObjectURL(url);
    } catch (err: any) {
      console.error(err);
      if (err.response?.data instanceof Blob) {
        const text = await err.response.data.text();
        try {
          const json = JSON.parse(text);
          alert('Failed to download: ' + (json.message || text));
        } catch (e) {
          alert('Failed to download: ' + text);
        }
      } else {
        alert('Failed to download object: ' + (err.response?.data?.message || err.message));
      }
    }
  };

  const handleCopy = (hash: string) => {
    navigator.clipboard.writeText(hash);
    setCopiedId(hash);
    setTimeout(() => setCopiedId(''), 2000);
  };

  const filtered = objects.filter((o: any) => {
    if (tierFilter !== 'ALL' && o.tier !== tierFilter) return false;
    if (search && o.object_name && !o.object_name.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-8">
      <div>
        <h1 className="text-3xl font-semibold text-white tracking-tight">Object Explorer</h1>
        <p className="text-gray-400 mt-1">Manage and retrieve stored files</p>
      </div>

      <div className="flex flex-col sm:flex-row justify-between gap-4">
        <div className="relative flex-1 max-w-md">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={18} />
          <input 
            type="text" 
            placeholder="Search objects..." 
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full bg-obsidian-light border border-gray-800 rounded-lg py-2.5 pl-10 pr-4 text-white focus:outline-none focus:border-primary transition-colors"
          />
        </div>
        
        <div className="flex gap-2 bg-obsidian-light p-1 rounded-lg border border-gray-800">
          {['ALL', 'HOT', 'WARM', 'COLD'].map(tier => (
            <button
              key={tier}
              onClick={() => setTierFilter(tier)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                tierFilter === tier ? 'bg-gray-800 text-white shadow-sm' : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/50'
              }`}
            >
              {tier}
            </button>
          ))}
        </div>
      </div>

      <div className="bg-obsidian-light border border-gray-800 rounded-2xl overflow-hidden">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-gray-900/50 border-b border-gray-800">
              <th className="px-6 py-4 text-sm font-medium text-gray-400">Object Name</th>
              <th className="px-6 py-4 text-sm font-medium text-gray-400">Size</th>
              <th className="px-6 py-4 text-sm font-medium text-gray-400">Tier</th>
              <th className="px-6 py-4 text-sm font-medium text-gray-400">SHA-256</th>
              <th className="px-6 py-4 text-sm font-medium text-gray-400 text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {filtered.map((obj: any) => (
              <tr key={obj.object_id} className="hover:bg-gray-800/30 transition-colors group">
                <td className="px-6 py-4">
                  <div className="flex items-center gap-3">
                    <FileText size={18} className="text-primary" />
                    <span className="font-medium text-gray-200">{obj.object_name}</span>
                  </div>
                </td>
                <td className="px-6 py-4 text-gray-400 text-sm">{((obj.file_size || 0) / 1024).toFixed(2)} KB</td>
                <td className="px-6 py-4">
                  <span className={`px-2.5 py-1 rounded-full text-xs font-medium border ${
                    obj.tier === 'HOT' ? 'border-red-500/30 text-red-400 bg-red-500/10' :
                    obj.tier === 'WARM' ? 'border-amber-500/30 text-amber-400 bg-amber-500/10' :
                    'border-blue-500/30 text-blue-400 bg-blue-500/10'
                  }`}>
                    {obj.tier || 'UNKNOWN'}
                  </span>
                </td>
                <td className="px-6 py-4">
                  <button 
                    onClick={() => handleCopy(obj.checksum || '')}
                    className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-300 font-mono bg-obsidian px-2 py-1 rounded border border-gray-800"
                    title={obj.checksum}
                  >
                    {(obj.checksum || '').substring(0, 8)}...
                    {copiedId === obj.checksum ? <Check size={14} className="text-emerald-400" /> : <Copy size={14} />}
                  </button>
                </td>
                <td className="px-6 py-4 text-right">
                  <div className="flex justify-end gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button 
                      onClick={() => handleDownload(obj.object_id, obj.object_name)}
                      className="p-2 text-gray-400 hover:text-primary hover:bg-primary/10 rounded-lg transition-colors"
                      title="Download"
                    >
                      <Download size={18} />
                    </button>
                    <button 
                      onClick={() => handleDelete(obj.object_id)}
                      className="p-2 text-gray-400 hover:text-red-400 hover:bg-red-400/10 rounded-lg transition-colors"
                      title="Cascade Purge"
                    >
                      <Trash2 size={18} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <div className="p-12 text-center text-gray-500">
            No objects found matching your criteria.
          </div>
        )}
      </div>
    </div>
  );
}
