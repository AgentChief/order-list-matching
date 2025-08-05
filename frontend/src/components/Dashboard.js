import React, { useState, useEffect } from 'react';
import { Row, Col, Card, Statistic, Progress, Table, Button, Space, Alert, Spin } from 'antd';
import { 
  DatabaseOutlined, 
  CheckCircleOutlined, 
  ClockCircleOutlined,
  WarningOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import ApiService from '../services/ApiService';

const Dashboard = ({ systemStatus, onStatusUpdate }) => {
  const [loading, setLoading] = useState(true);
  const [dashboardData, setDashboardData] = useState({
    totalMovements: 0,
    totalMatches: 0,
    matchRate: 0,
    hitlQueueSize: 0,
    recentActivity: [],
    layerPerformance: [],
    topCustomers: []
  });
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    loadDashboardData();
  }, []);

  const loadDashboardData = async () => {
    try {
      setLoading(true);
      
      // Load dashboard data
      const data = await ApiService.getDashboardData();
      setDashboardData(data);
      
      // Update system status
      const status = await ApiService.getSystemStatus();
      onStatusUpdate(status);
      
    } catch (error) {
      console.error('Error loading dashboard:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleRefresh = async () => {
    setRefreshing(true);
    await loadDashboardData();
    setRefreshing(false);
  };

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8'];

  const recentActivityColumns = [
    {
      title: 'Time',
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 150,
      render: (text) => new Date(text).toLocaleTimeString()
    },
    {
      title: 'Activity',
      dataIndex: 'activity',
      key: 'activity'
    },
    {
      title: 'Customer',
      dataIndex: 'customer',
      key: 'customer',
      width: 120
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status) => (
        <span style={{ 
          color: status === 'success' ? '#52c41a' : status === 'error' ? '#ff4d4f' : '#1890ff'
        }}>
          {status.toUpperCase()}
        </span>
      )
    }
  ];

  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '400px' }}>
        <Spin size="large" />
      </div>
    );
  }

  return (
    <div>
      {/* Header Actions */}
      <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1 style={{ margin: 0 }}>System Overview</h1>
        <Button 
          type="primary" 
          icon={<ReloadOutlined />} 
          onClick={handleRefresh}
          loading={refreshing}
        >
          Refresh
        </Button>
      </div>

      {/* System Status Alert */}
      {!systemStatus.connected && (
        <Alert
          message="Database Connection Issue"
          description="Unable to connect to the database. Some features may not be available."
          type="error"
          showIcon
          style={{ marginBottom: '24px' }}
        />
      )}

      {/* Key Metrics */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Total Movements"
              value={dashboardData.totalMovements}
              prefix={<DatabaseOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Total Matches"
              value={dashboardData.totalMatches}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Match Rate"
              value={dashboardData.matchRate}
              suffix="%"
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: dashboardData.matchRate > 90 ? '#52c41a' : '#faad14' }}
            />
            <Progress 
              percent={dashboardData.matchRate} 
              strokeColor={dashboardData.matchRate > 90 ? '#52c41a' : '#faad14'}
              showInfo={false}
              size="small"
              style={{ marginTop: '8px' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="HITL Queue"
              value={dashboardData.hitlQueueSize}
              prefix={<ClockCircleOutlined />}
              valueStyle={{ color: dashboardData.hitlQueueSize > 10 ? '#ff4d4f' : '#1890ff' }}
            />
            {dashboardData.hitlQueueSize > 10 && (
              <div style={{ fontSize: '12px', color: '#ff4d4f', marginTop: '4px' }}>
                <WarningOutlined /> Attention Required
              </div>
            )}
          </Card>
        </Col>
      </Row>

      {/* Charts Section */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} lg={12}>
          <Card title="Layer Performance" bordered={false}>
            {dashboardData.layerPerformance.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={dashboardData.layerPerformance}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="layer" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="matches" fill="#1890ff" name="Matches" />
                  <Bar dataKey="confidence" fill="#52c41a" name="Avg Confidence" />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ textAlign: 'center', padding: '40px', color: '#999' }}>
                No layer performance data available
              </div>
            )}
          </Card>
        </Col>
        
        <Col xs={24} lg={12}>
          <Card title="Customer Distribution" bordered={false}>
            {dashboardData.topCustomers.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={dashboardData.topCustomers}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {dashboardData.topCustomers.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ textAlign: 'center', padding: '40px', color: '#999' }}>
                No customer data available
              </div>
            )}
          </Card>
        </Col>
      </Row>

      {/* Recent Activity */}
      <Card title="Recent Activity" bordered={false}>
        <Table 
          columns={recentActivityColumns} 
          dataSource={dashboardData.recentActivity}
          pagination={{ pageSize: 10, showSizeChanger: false }}
          size="small"
          rowKey="id"
          locale={{
            emptyText: 'No recent activity'
          }}
        />
      </Card>

      {/* Quick Actions */}
      <Card title="Quick Actions" bordered={false} style={{ marginTop: '16px' }}>
        <Space wrap>
          <Button type="primary" onClick={() => window.location.hash = '#/matching-engine'}>
            Run Matching
          </Button>
          <Button onClick={() => window.location.hash = '#/hitl-review'}>
            Review Queue
          </Button>
          <Button onClick={() => window.location.hash = '#/procedure-runner'}>
            Run Procedures
          </Button>
          <Button onClick={() => ApiService.refreshCache()}>
            Refresh Cache
          </Button>
        </Space>
      </Card>
    </div>
  );
};

export default Dashboard;