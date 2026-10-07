import React, { useState, useEffect } from 'react';
import { Database, HardDrive, Activity, PieChart } from 'lucide-react';
import api from '../api';
import { Link } from 'react-router-dom';

export default function Dashboard() {
  const [stats, setStats] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await api.get('/cluster/status');
        setStats(res.data.data);
      } catch (err) {
        console.error('Failed to fetch stats', err);
      } finally {
        setLoading(false);
      }
    };
    fetchStatus();
    const interval = setInterval(fetchStatus, 10000);
    return () => clearInterval(interval);
  }, []);

  if (loading && !stats) {
    return (
      <div className="p-8 space-y-6">
        <h1 className="text-2xl font-semibold">Overview</h1>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {[1, 2, 3, 4].map(i => (
            <div key={i} className="bg-obsidian-light p-6 rounded-2xl border border-gray-800 animate-pulse h-32"></div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-8">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-semibold text-white tracking-tight">Overview</h1>
          <p className="text-gray-400 mt-1">Live telemetry for your storage cluster</p>
        </div>
        <Link to="/upload" className="bg-primary hover:bg-blue-600 text-white px-5 py-2.5 rounded-lg font-medium transition-colors shadow-lg shadow-primary/20">
          Quick Upload
        </Link>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard 
          title="Stored Objects" 
          value={stats?.total_objects || '0'} 
          icon={<Database className="text-primary" />} 
        />
        <MetricCard 
          title="Physical Capacity" 
          value={`${((stats?.used_storage_bytes || 0) / (1024 * 1024)).toFixed(2)} MB`} 
          subtitle={`of ${((stats?.total_storage_bytes || 1000000) / (1024 * 1024)).toFixed(0)} MB`}
          icon={<HardDrive className="text-emerald-400" />} 
        />
        <MetricCard 
          title="Online Fleet" 
          value={stats?.online_nodes || '0'} 
          subtitle="Storage nodes active"
          icon={<Activity className="text-purple-400" />} 
        />
        <MetricCard 
          title="Cluster Health" 
          value={stats?.status || 'UNKNOWN'} 
          icon={<PieChart className="text-amber-400" />} 
        />
      </div>
      
      <div className="bg-obsidian-light border border-gray-800 rounded-2xl p-6">
        <h2 className="text-xl font-semibold mb-4">Tier Breakdown</h2>
        <div className="flex flex-col space-y-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-2">
              <div className="w-3 h-3 rounded-full bg-red-500"></div>
              <span className="text-gray-300">Hot Tier (Active, 5x Replication)</span>
            </div>
            <span className="font-semibold">{stats?.tier_hot || 0} objects</span>
          </div>
          <div className="w-full bg-obsidian rounded-full h-2">
            <div className="bg-red-500 h-2 rounded-full" style={{ width: `${Math.max(2, ((stats?.tier_hot || 0) / Math.max(1, (stats?.total_objects || 1))) * 100)}%` }}></div>
          </div>

          <div className="flex items-center justify-between mt-4">
            <div className="flex items-center space-x-2">
              <div className="w-3 h-3 rounded-full bg-amber-500"></div>
              <span className="text-gray-300">Warm Tier (Standard, 3x Replication)</span>
            </div>
            <span className="font-semibold">{stats?.tier_warm || 0} objects</span>
          </div>
          <div className="w-full bg-obsidian rounded-full h-2">
            <div className="bg-amber-500 h-2 rounded-full" style={{ width: `${Math.max(2, ((stats?.tier_warm || 0) / Math.max(1, (stats?.total_objects || 1))) * 100)}%` }}></div>
          </div>

          <div className="flex items-center justify-between mt-4">
            <div className="flex items-center space-x-2">
              <div className="w-3 h-3 rounded-full bg-blue-500"></div>
              <span className="text-gray-300">Cold Tier (Archive, 2x Replication)</span>
            </div>
            <span className="font-semibold">{stats?.tier_cold || 0} objects</span>
          </div>
          <div className="w-full bg-obsidian rounded-full h-2">
            <div className="bg-blue-500 h-2 rounded-full" style={{ width: `${Math.max(2, ((stats?.tier_cold || 0) / Math.max(1, (stats?.total_objects || 1))) * 100)}%` }}></div>
          </div>
        </div>
      </div>
    </div>
  );
}

function MetricCard({ title, value, subtitle, icon }: any) {
  return (
    <div className="bg-obsidian-light p-6 rounded-2xl border border-gray-800 hover:border-gray-700 transition-colors">
      <div className="flex justify-between items-start mb-4">
        <h3 className="text-gray-400 font-medium">{title}</h3>
        <div className="p-2 bg-obsidian rounded-lg border border-gray-800">
          {icon}
        </div>
      </div>
      <div className="text-3xl font-semibold text-white">{value}</div>
      {subtitle && <div className="text-sm text-gray-500 mt-2">{subtitle}</div>}
    </div>
  );
}
