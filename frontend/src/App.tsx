import React, { useState, useEffect } from 'react';
import { BrowserRouter, Routes, Route, Navigate, Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, Database, UploadCloud, Activity, LogOut, HardDrive } from 'lucide-react';
import Login from './components/Login';
import Dashboard from './components/Dashboard';
import ObjectExplorer from './components/ObjectExplorer';
import Upload from './components/Upload';
import Admin from './components/Admin';

const Sidebar = ({ onLogout }: { onLogout: () => void }) => {
  const location = useLocation();
  const token = localStorage.getItem('token');
  const role = localStorage.getItem('role');
  
  if (!token) return null;

  const navItems = [
    { path: '/', icon: <LayoutDashboard size={20} />, label: 'Dashboard' },
    { path: '/explorer', icon: <Database size={20} />, label: 'Object Explorer' },
    { path: '/upload', icon: <UploadCloud size={20} />, label: 'Upload' },
  ];

  if (role === 'ADMIN') {
    navItems.push({ path: '/admin', icon: <Activity size={20} />, label: 'Cluster Admin' });
  }

  return (
    <div className="w-64 bg-obsidian-light border-r border-gray-800 h-screen flex flex-col">
      <div className="p-6 flex items-center gap-3 border-b border-gray-800">
        <div className="bg-primary/20 p-2 rounded-lg text-primary">
          <HardDrive size={24} />
        </div>
        <h1 className="font-semibold text-lg leading-tight">Distributed Storage</h1>
      </div>
      <nav className="flex-1 p-4 space-y-2">
        {navItems.map((item) => {
          const active = location.pathname === item.path;
          return (
            <Link 
              key={item.path} 
              to={item.path}
              className={`flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${active ? 'bg-primary/10 text-primary' : 'text-gray-400 hover:text-gray-100 hover:bg-gray-800/50'}`}
            >
              {item.icon}
              <span className="font-medium">{item.label}</span>
            </Link>
          );
        })}
      </nav>
      <div className="p-4 border-t border-gray-800">
        <button 
          onClick={onLogout}
          className="flex items-center gap-3 px-4 py-3 w-full rounded-lg text-gray-400 hover:text-red-400 hover:bg-red-400/10 transition-colors"
        >
          <LogOut size={20} />
          <span className="font-medium">Sign Out</span>
        </button>
      </div>
    </div>
  );
};

export default function App() {
  const [authChanged, setAuthChanged] = useState(0);
  const token = localStorage.getItem('token');

  const handleAuthChange = () => setAuthChanged(prev => prev + 1);
  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    handleAuthChange();
  };

  return (
    <BrowserRouter>
      <div className="flex min-h-screen bg-obsidian text-gray-100">
        {token && <Sidebar onLogout={handleLogout} />}
        <main className="flex-1 overflow-auto h-screen">
          <Routes>
            {!token ? (
              <>
                <Route path="/login" element={<Login onLogin={handleAuthChange} />} />
                <Route path="*" element={<Navigate to="/login" replace />} />
              </>
            ) : (
              <>
                <Route path="/" element={<Dashboard />} />
                <Route path="/explorer" element={<ObjectExplorer />} />
                <Route path="/upload" element={<Upload />} />
                <Route path="/admin" element={<Admin />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </>
            )}
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
