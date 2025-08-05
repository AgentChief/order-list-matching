import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Layout, Menu, theme, notification, Spin } from 'antd';
import {
  DashboardOutlined,
  DatabaseOutlined,
  SettingOutlined,
  TableOutlined,
  PlayCircleOutlined,
  BarChartOutlined,
  CheckCircleOutlined
} from '@ant-design/icons';

// Import components
import Dashboard from './components/Dashboard';
import DataViewer from './components/DataViewer';
import QueueManager from './components/QueueManager';
import MatchingEngine from './components/MatchingEngine';
import ProcedureRunner from './components/ProcedureRunner';
import Analytics from './components/Analytics';
import HitlReview from './components/HitlReview';

// Import API service
import ApiService from './services/ApiService';

import './App.css';

const { Header, Sider, Content } = Layout;

function App() {
  const [collapsed, setCollapsed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [currentPage, setCurrentPage] = useState('dashboard');
  const [systemStatus, setSystemStatus] = useState({
    connected: false,
    lastUpdate: null,
    totalMovements: 0,
    totalMatches: 0
  });

  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken();

  // Initialize app and check database connection
  useEffect(() => {
    initializeApp();
  }, []);

  const initializeApp = async () => {
    try {
      setLoading(true);
      
      // Test database connection and get system status
      const status = await ApiService.getSystemStatus();
      setSystemStatus(status);
      
      notification.success({
        message: 'System Connected',
        description: 'Successfully connected to database and loaded system status.',
        duration: 3
      });
    } catch (error) {
      console.error('Failed to initialize app:', error);
      notification.error({
        message: 'Connection Failed',
        description: 'Failed to connect to database. Please check your connection settings.',
        duration: 5
      });
    } finally {
      setLoading(false);
    }
  };

  const menuItems = [
    {
      key: 'dashboard',
      icon: <DashboardOutlined />,
      label: 'Dashboard',
      title: 'System Overview'
    },
    {
      key: 'data-viewer',
      icon: <TableOutlined />,
      label: 'Data Viewer',
      title: 'View & Edit Data'
    },
    {
      key: 'queue-manager',
      icon: <DatabaseOutlined />,
      label: 'Queue Manager',
      title: 'Manage Queues'
    },
    {
      key: 'hitl-review',
      icon: <CheckCircleOutlined />,
      label: 'HITL Review',
      title: 'Human Review'
    },
    {
      key: 'matching-engine',
      icon: <PlayCircleOutlined />,
      label: 'Matching Engine',
      title: 'Run Matching'
    },
    {
      key: 'procedure-runner',
      icon: <SettingOutlined />,
      label: 'Procedures',
      title: 'Run Procedures'
    },
    {
      key: 'analytics',
      icon: <BarChartOutlined />,
      label: 'Analytics',
      title: 'Performance Analytics'
    }
  ];

  const getCurrentPageTitle = () => {
    const item = menuItems.find(item => item.key === currentPage);
    return item ? item.title : 'Order Matching System';
  };

  const renderCurrentPage = () => {
    const commonProps = {
      systemStatus,
      onStatusUpdate: setSystemStatus
    };

    switch (currentPage) {
      case 'dashboard':
        return <Dashboard {...commonProps} />;
      case 'data-viewer':
        return <DataViewer {...commonProps} />;
      case 'queue-manager':
        return <QueueManager {...commonProps} />;
      case 'hitl-review':
        return <HitlReview {...commonProps} />;
      case 'matching-engine':
        return <MatchingEngine {...commonProps} />;
      case 'procedure-runner':
        return <ProcedureRunner {...commonProps} />;
      case 'analytics':
        return <Analytics {...commonProps} />;
      default:
        return <Dashboard {...commonProps} />;
    }
  };

  if (loading) {
    return (
      <div style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        height: '100vh',
        flexDirection: 'column',
        gap: '16px'
      }}>
        <Spin size="large" />
        <p>Initializing Order Matching System...</p>
      </div>
    );
  }

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider 
        trigger={null} 
        collapsible 
        collapsed={collapsed}
        style={{
          overflow: 'auto',
          height: '100vh',
          position: 'fixed',
          left: 0,
          top: 0,
          bottom: 0,
        }}
      >
        <div style={{ 
          height: '32px', 
          margin: '16px',
          background: 'rgba(255, 255, 255, 0.2)',
          borderRadius: '6px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: 'white',
          fontWeight: 'bold'
        }}>
          {collapsed ? 'OMS' : 'Order Matching'}
        </div>
        
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[currentPage]}
          items={menuItems}
          onClick={({ key }) => setCurrentPage(key)}
        />
      </Sider>
      
      <Layout style={{ marginLeft: collapsed ? 80 : 200, transition: 'margin-left 0.2s' }}>
        <Header
          style={{
            padding: '0 24px',
            background: colorBgContainer,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            boxShadow: '0 1px 4px rgba(0,21,41,.08)'
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <h2 style={{ margin: 0, color: '#1890ff' }}>
              {getCurrentPageTitle()}
            </h2>
          </div>
          
          <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
            <div style={{ 
              padding: '4px 12px', 
              borderRadius: '4px',
              background: systemStatus.connected ? '#f6ffed' : '#fff2f0',
              border: `1px solid ${systemStatus.connected ? '#b7eb8f' : '#ffb3b3'}`,
              color: systemStatus.connected ? '#52c41a' : '#ff4d4f',
              fontSize: '12px'
            }}>
              {systemStatus.connected ? '🟢 Connected' : '🔴 Disconnected'}
            </div>
            
            {systemStatus.lastUpdate && (
              <div style={{ fontSize: '12px', color: '#666' }}>
                Last Update: {new Date(systemStatus.lastUpdate).toLocaleTimeString()}
              </div>
            )}
          </div>
        </Header>
        
        <Content
          style={{
            margin: '24px 24px 0',
            padding: 24,
            minHeight: 280,
            background: colorBgContainer,
            borderRadius: borderRadiusLG,
            overflow: 'auto'
          }}
        >
          {renderCurrentPage()}
        </Content>
      </Layout>
    </Layout>
  );
}

export default App;