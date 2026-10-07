import React, { useState, useEffect, useRef } from 'react';
import { UploadCloud, CheckCircle2, XCircle } from 'lucide-react';
import api from '../api';

export default function Upload() {
  const [file, setFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState<'IDLE' | 'HASHING' | 'UPLOADING' | 'SUCCESS' | 'ERROR'>('IDLE');
  const [message, setMessage] = useState('');
  const fileInputRef = useRef<HTMLInputElement>(null);

  const calculateSHA256 = async (file: File): Promise<string> => {
    const buffer = await file.arrayBuffer();
    const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
  };

  const handleUpload = async () => {
    if (!file) return;
    try {
      setStatus('HASHING');
      const hash = await calculateSHA256(file);
      
      setStatus('UPLOADING');
      setProgress(10);
      
      const formData = new FormData();
      formData.append('file', file);
      formData.append('checksum', hash);
      
      const res = await api.post('/objects', formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        onUploadProgress: (e) => {
          if (e.total) setProgress(10 + Math.round((e.loaded * 90) / e.total));
        }
      });
      
      setStatus('SUCCESS');
      setMessage(`Object created successfully. ID: ${res.data.data.object_id}`);
      setFile(null);
      setProgress(100);
    } catch (err: any) {
      setStatus('ERROR');
      setMessage(err.response?.data?.message || err.message || 'Upload failed');
    }
  };

  const onDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };
  const onDragLeave = () => setIsDragging(false);
  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files?.[0]) setFile(e.dataTransfer.files[0]);
  };

  return (
    <div className="p-8 max-w-4xl mx-auto">
      <h1 className="text-3xl font-semibold mb-2">Upload Object</h1>
      <p className="text-gray-400 mb-8">Securely upload files with browser-native SHA-256 pre-calculation.</p>
      
      <div 
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onDrop={onDrop}
        onClick={() => fileInputRef.current?.click()}
        className={`border-2 border-dashed rounded-3xl p-12 text-center cursor-pointer transition-all duration-200 ${
          isDragging 
            ? 'border-primary bg-primary/5' 
            : 'border-gray-700 bg-obsidian-light hover:border-gray-500 hover:bg-gray-800/50'
        }`}
      >
        <input 
          type="file" 
          className="hidden" 
          ref={fileInputRef}
          onChange={(e) => e.target.files?.[0] && setFile(e.target.files[0])}
        />
        <UploadCloud className="mx-auto h-16 w-16 text-gray-400 mb-6" />
        <h3 className="text-xl font-medium text-white mb-2">
          {file ? file.name : 'Drag and drop a file here'}
        </h3>
        <p className="text-gray-500">
          {file ? `${(file.size / 1024).toFixed(2)} KB` : 'or click to browse from your computer'}
        </p>
      </div>

      {file && status === 'IDLE' && (
        <div className="mt-8 flex justify-end">
          <button 
            onClick={handleUpload}
            className="bg-primary hover:bg-blue-600 text-white px-8 py-3 rounded-xl font-medium transition-colors flex items-center gap-2"
          >
            <UploadCloud size={20} /> Start Upload
          </button>
        </div>
      )}

      {status !== 'IDLE' && (
        <div className="mt-8 bg-obsidian-light border border-gray-800 rounded-2xl p-6">
          <div className="flex justify-between text-sm font-medium mb-4">
            <span className={status === 'SUCCESS' ? 'text-emerald-400' : status === 'ERROR' ? 'text-red-400' : 'text-primary'}>
              {status === 'HASHING' && 'Calculating SHA-256 Hash...'}
              {status === 'UPLOADING' && 'Allocating and Uploading...'}
              {status === 'SUCCESS' && 'Upload Complete!'}
              {status === 'ERROR' && 'Upload Failed'}
            </span>
            <span className="text-gray-400">{progress}%</span>
          </div>
          <div className="w-full bg-obsidian rounded-full h-3 border border-gray-800 overflow-hidden">
            <div 
              className={`h-full rounded-full transition-all duration-300 ${status === 'SUCCESS' ? 'bg-emerald-500' : status === 'ERROR' ? 'bg-red-500' : 'bg-primary'}`} 
              style={{ width: `${progress}%` }} 
            />
          </div>
          {message && (
            <div className={`mt-4 flex items-start gap-3 p-4 rounded-xl ${status === 'SUCCESS' ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400'}`}>
              {status === 'SUCCESS' ? <CheckCircle2 size={20} /> : <XCircle size={20} />}
              <p className="text-sm">{message}</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
