import React, { useState, useEffect } from 'react';
import { 
  Card, 
  Row, 
  Col, 
  Statistic, 
  Select, 
  Button, 
  Space,
  Table,
  Tag,
  Progress,
  Alert
} from 'antd';
import { 
  BarChartOutlined, 
  ReloadOutlined,
  TrophyOutlined,
  ClockCircleOutlined,
  CheckCircleOutlined
} from '@ant-design/icons';
import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend, 
  ResponsiveContainer,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  Area,
  AreaChart
} from 'recharts';
import ApiService from '../services/ApiService';

const { Option } = Select;

const Analytics = () => {
  const [loading, setLoading] = useState(false);
  const [timeRange, setTimeRange] = useState('30');
  const [analyticsData, setAnalyticsData] = useState({
    layerPerformance: [],
    customerPerformance: [],
    matchingTrends: [],
    systemPerformance: {}
  });

  useEffect(() => {
    loadAnalyticsData();
  }, [timeRange]);

  const loadAnalyticsData = async () => {
    try {
      setLoading(true);
      
      const [layerData, customerData, trendsData, systemData] = await Promise.all([
        ApiService.getLayerPerformance(),
        ApiService.getCustomerPerformance(),
        ApiService.getMatchingTrends(parseInt(timeRange)),
        ApiService.getSystemPerformance()
      ]);

      setAnalyticsData({
        layerPerformance: layerData || [],
        customerPerformance: customerData || [],
        matchingTrends: trendsData || [],
        systemPerformance: systemData || {}
      });

    } catch (error) {
      console.error('Failed to load analytics data:', error);
      // Set demo data for development
      setAnalyticsData({
        layerPerformance: [
          { layer: 'LAYER_0', matches: 1250, avgConfidence: 1.0, successRate: 100 },
          { layer: 'LAYER_1', matches: 890, avgConfidence: 0.92, successRate: 95 },
          { layer: 'LAYER_2', matches: 456, avgConfidence: 0.78, successRate: 87 },
          { layer: 'LAYER_3', matches: 123, avgConfidence: 0.65, successRate: 72 }
        ],
        customerPerformance: [
          { customer: 'GREYSON', totalMatches: 1450, avgConfidence: 0.89, issues: 23 },
          { customer: 'JOHNNIE_O', totalMatches: 980, avgConfidence: 0.91, issues: 12 },
          { customer: 'CUSTOMER_C', totalMatches: 756, avgConfidence: 0.85, issues: 34 },
          { customer: 'CUSTOMER_D', totalMatches: 543, avgConfidence: 0.88, issues: 18 }
        ],
        matchingTrends: [
          { date: '2025-03-01', matches: 145, rate: 91.2 },
          { date: '2025-03-02', matches: 167, rate: 93.1 },
          { date: '2025-03-03', matches: 134, rate: 89.7 },
          { date: '2025-03-04', matches: 178, rate: 94.5 },
          { date: '2025-03-05', matches: 156, rate: 92.3 }
        ],
        systemPerformance: {
          avgQueryTime: 0.18,
          cacheHitRate: 98.5,
          matchingThroughput: 1245,
          systemLoad: 68
        }
      });
    } finally {
      setLoading(false);
    }
  };

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8'];

  const layerColumns = [
    {
      title: 'Layer',
      dataIndex: 'layer',
      key: 'layer',
      render: (layer) => (
        <Tag color={
          layer === 'LAYER_0' ? 'green' :
          layer === 'LAYER_1' ? 'blue' :
          layer === 'LAYER_2' ? 'orange' : 'red'
        }>
          {layer}
        </Tag>
      )
    },
    {
      title: 'Total Matches',
      dataIndex: 'matches',
      key: 'matches',
      sorter: (a, b) => a.matches - b.matches
    },
    {
      title: 'Avg Confidence',
      dataIndex: 'avgConfidence',
      key: 'avgConfidence',
      render: (confidence) => `${(confidence * 100).toFixed(1)}%`,
      sorter: (a, b) => a.avgConfidence - b.avgConfidence
    },
    {
      title: 'Success Rate',
      dataIndex: 'successRate',
      key: 'successRate',
      render: (rate) => (
        <div>
          <Progress 
            percent={rate} 
            size="small" 
            strokeColor={rate > 90 ? '#52c41a' : rate > 80 ? '#faad14' : '#ff4d4f'}
          />
        </div>
      ),
      sorter: (a, b) => a.successRate - b.successRate
    }
  ];

  const customerColumns = [
    {
      title: 'Customer',
      dataIndex: 'customer',
      key: 'customer'
    },
    {
      title: 'Total Matches',
      dataIndex: 'totalMatches',
      key: 'totalMatches',
      sorter: (a, b) => a.totalMatches - b.totalMatches
    },
    {
      title: 'Avg Confidence',
      dataIndex: 'avgConfidence',
      key: 'avgConfidence',
      render: (confidence) => `${(confidence * 100).toFixed(1)}%`,
      sorter: (a, b) => a.avgConfidence - b.avgConfidence
    },
    {
      title: 'Issues',
      dataIndex: 'issues',
      key: 'issues',
      render: (issues) => (
        <Tag color={issues === 0 ? 'green' : issues < 20 ? 'orange' : 'red'}>
          {issues}
        </Tag>
      ),
      sorter: (a, b) => a.issues - b.issues
    },
    {
      title: 'Quality Score',
      key: 'qualityScore',
      render: (_, record) => {
        const score = Math.round((record.avgConfidence * 100) - (record.issues / record.totalMatches * 100));
        return (
          <Progress 
            percent={score} 
            size="small" 
            strokeColor={score > 85 ? '#52c41a' : score > 70 ? '#faad14' : '#ff4d4f'}
          />
        );
      },
      sorter: (a, b) => {
        const scoreA = (a.avgConfidence * 100) - (a.issues / a.totalMatches * 100);
        const scoreB = (b.avgConfidence * 100) - (b.issues / b.totalMatches * 100);
        return scoreA - scoreB;
      }
    }
  ];

  return (
    <div>
      {/* Header Controls */}
      <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1 style={{ margin: 0 }}>Performance Analytics</h1>
        <Space>
          <Select
            value={timeRange}
            onChange={setTimeRange}
            style={{ width: '150px' }}
          >
            <Option value="7">Last 7 days</Option>
            <Option value="30">Last 30 days</Option>
            <Option value="90">Last 90 days</Option>
          </Select>
          <Button 
            icon={<ReloadOutlined />} 
            onClick={loadAnalyticsData}
            loading={loading}
          >
            Refresh
          </Button>
        </Space>
      </div>

      {/* System Performance Metrics */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Avg Query Time"
              value={analyticsData.systemPerformance.avgQueryTime}
              suffix="s"
              precision={2}
              prefix={<ClockCircleOutlined />}
              valueStyle={{ color: analyticsData.systemPerformance.avgQueryTime < 1 ? '#52c41a' : '#faad14' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Cache Hit Rate"
              value={analyticsData.systemPerformance.cacheHitRate}
              suffix="%"
              precision={1}
              prefix={<TrophyOutlined />}
              valueStyle={{ color: analyticsData.systemPerformance.cacheHitRate > 95 ? '#52c41a' : '#faad14' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Matching Throughput"
              value={analyticsData.systemPerformance.matchingThroughput}
              suffix="/hr"
              prefix={<BarChartOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="System Load"
              value={analyticsData.systemPerformance.systemLoad}
              suffix="%"
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: analyticsData.systemPerformance.systemLoad < 80 ? '#52c41a' : '#faad14' }}
            />
          </Card>
        </Col>
      </Row>

      {/* Performance Alerts */}
      {analyticsData.systemPerformance.avgQueryTime > 1 && (
        <Alert
          message="Performance Alert"
          description="Average query time is above 1 second. Consider cache optimization."
          type="warning"
          showIcon
          style={{ marginBottom: '24px' }}
        />
      )}

      {/* Charts Section */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        {/* Layer Performance Chart */}
        <Col xs={24} lg={12}>
          <Card title="🎯 Layer Performance Distribution">
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={analyticsData.layerPerformance}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="layer" />
                <YAxis yAxisId="left" />
                <YAxis yAxisId="right" orientation="right" />
                <Tooltip />
                <Legend />
                <Bar yAxisId="left" dataKey="matches" fill="#1890ff" name="Total Matches" />
                <Bar yAxisId="right" dataKey="successRate" fill="#52c41a" name="Success Rate %" />
              </BarChart>
            </ResponsiveContainer>
          </Card>
        </Col>

        {/* Customer Performance Pie Chart */}
        <Col xs={24} lg={12}>
          <Card title="👥 Customer Match Distribution">
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={analyticsData.customerPerformance}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ customer, totalMatches, percent }) => 
                    `${customer}: ${totalMatches} (${(percent * 100).toFixed(0)}%)`
                  }
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="totalMatches"
                >
                  {analyticsData.customerPerformance.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Card>
        </Col>
      </Row>

      {/* Matching Trends */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24}>
          <Card title="📈 Matching Trends Over Time">
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={analyticsData.matchingTrends}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis yAxisId="left" />
                <YAxis yAxisId="right" orientation="right" />
                <Tooltip />
                <Legend />
                <Area yAxisId="left" type="monotone" dataKey="matches" stackId="1" stroke="#1890ff" fill="#1890ff" fillOpacity={0.6} name="Daily Matches" />
                <Line yAxisId="right" type="monotone" dataKey="rate" stroke="#52c41a" strokeWidth={3} name="Match Rate %" />
              </AreaChart>
            </ResponsiveContainer>
          </Card>
        </Col>
      </Row>

      {/* Detailed Performance Tables */}
      <Row gutter={[16, 16]}>
        <Col xs={24} lg={12}>
          <Card title="🎯 Layer Performance Details">
            <Table
              columns={layerColumns}
              dataSource={analyticsData.layerPerformance}
              pagination={false}
              size="small"
              rowKey="layer"
            />
          </Card>
        </Col>

        <Col xs={24} lg={12}>
          <Card title="👥 Customer Performance Details">
            <Table
              columns={customerColumns}
              dataSource={analyticsData.customerPerformance}
              pagination={false}
              size="small"
              rowKey="customer"
            />
          </Card>
        </Col>
      </Row>

      {/* Performance Insights */}
      <Card title="💡 Performance Insights" style={{ marginTop: '24px' }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} md={8}>
            <div style={{ textAlign: 'center', padding: '16px' }}>
              <div style={{ fontSize: '24px', color: '#52c41a', marginBottom: '8px' }}>
                <TrophyOutlined />
              </div>
              <h4>Best Performing Layer</h4>
              <p>
                {analyticsData.layerPerformance.length > 0 && 
                  analyticsData.layerPerformance.reduce((best, current) => 
                    current.successRate > best.successRate ? current : best
                  ).layer
                } achieves highest success rate
              </p>
            </div>
          </Col>

          <Col xs={24} md={8}>
            <div style={{ textAlign: 'center', padding: '16px' }}>
              <div style={{ fontSize: '24px', color: '#1890ff', marginBottom: '8px' }}>
                <BarChartOutlined />
              </div>
              <h4>Top Customer</h4>
              <p>
                {analyticsData.customerPerformance.length > 0 && 
                  analyticsData.customerPerformance.reduce((top, current) => 
                    current.totalMatches > top.totalMatches ? current : top
                  ).customer
                } has most matches processed
              </p>
            </div>
          </Col>

          <Col xs={24} md={8}>
            <div style={{ textAlign: 'center', padding: '16px' }}>
              <div style={{ fontSize: '24px', color: '#faad14', marginBottom: '8px' }}>
                <ClockCircleOutlined />
              </div>
              <h4>System Health</h4>
              <p>
                {analyticsData.systemPerformance.systemLoad < 80 ? 'Optimal' : 'High'} system load
                <br />
                {analyticsData.systemPerformance.cacheHitRate > 95 ? 'Excellent' : 'Good'} cache performance
              </p>
            </div>
          </Col>
        </Row>
      </Card>
    </div>
  );
};

export default Analytics;