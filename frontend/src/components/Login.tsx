import React, { useState } from 'react';
import { Lock, Mail, Server } from 'lucide-react';
import api from '../api';

export default function Login({ onLogin }: { onLogin: () => void }) {
  const [isRegister, setIsRegister] = useState(false);
  const [email, setEmail] = useState('demo@example.com');
  const [password, setPassword] = useState('password123');
  const [role, setRole] = useState<'USER' | 'ADMIN'>('USER');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    
    try {
      if (isRegister) {
        await api.post('/auth/register', { email, password, role });
      }
      const res = await api.post('/auth/login', { email, password });
      
      if (res.data?.data?.token) {
        localStorage.setItem('token', res.data.data.token);
        localStorage.setItem('role', res.data.data.user.role);
        onLogin();
      } else {
        throw new Error('Invalid response');
      }
    } catch (err: any) {
      setError(err.response?.data?.message || err.message || 'Authentication failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-obsidian p-4">
      <div className="w-full max-w-md bg-obsidian-light rounded-2xl border border-gray-800 p-8 shadow-2xl">
        <div className="flex flex-col items-center mb-8">
          <div className="w-16 h-16 bg-primary/10 rounded-2xl flex items-center justify-center mb-4 text-primary">
            <Server size={32} />
          </div>
          <h2 className="text-2xl font-semibold text-white">
            Distributed Storage
          </h2>
          <p className="text-gray-400 mt-2 text-center">
            {isRegister ? 'Create a new account to join the cluster' : 'Sign in to access your objects and telemetry'}
          </p>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 text-sm">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-5">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Email</label>
            <div className="relative">
              <Mail className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={18} />
              <input 
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
                className="w-full bg-obsidian border border-gray-800 rounded-lg py-2.5 pl-10 pr-4 text-white placeholder-gray-500 focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary transition-colors"
                placeholder="admin@example.com"
              />
            </div>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Password</label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={18} />
              <input 
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                className="w-full bg-obsidian border border-gray-800 rounded-lg py-2.5 pl-10 pr-4 text-white placeholder-gray-500 focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary transition-colors"
                placeholder="••••••••"
              />
            </div>
          </div>

          {isRegister && (
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1.5">Role</label>
              <div className="grid grid-cols-2 gap-3">
                {(['USER', 'ADMIN'] as const).map(r => (
                  <button
                    key={r}
                    type="button"
                    onClick={() => setRole(r)}
                    className={`py-2 px-4 rounded-lg border text-sm font-medium transition-colors ${
                      role === r 
                        ? 'border-primary bg-primary/10 text-primary' 
                        : 'border-gray-800 bg-obsidian text-gray-400 hover:border-gray-700'
                    }`}
                  >
                    {r}
                  </button>
                ))}
              </div>
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-primary hover:bg-blue-600 text-white font-medium py-2.5 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            {loading ? (
              <span className="w-5 h-5 border-2 border-white/20 border-t-white rounded-full animate-spin" />
            ) : (
              isRegister ? 'Create Account' : 'Sign In'
            )}
          </button>
        </form>

        <p className="mt-6 text-center text-sm text-gray-400">
          {isRegister ? 'Already have an account? ' : "Don't have an account? "}
          <button 
            onClick={() => setIsRegister(!isRegister)}
            className="text-primary hover:text-blue-400 font-medium transition-colors"
          >
            {isRegister ? 'Sign In' : 'Create one'}
          </button>
        </p>
      </div>
    </div>
  );
}
