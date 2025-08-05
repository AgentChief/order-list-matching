import React, { useState, useEffect } from 'react';
import { 
  Card, 
  Form, 
  Input, 
  Button, 
  Select, 
  Space, 
  message, 
  Progress,
  Table,
  Row,
  Col,
  Statistic,
  Timeline,
  Tag,
  Alert,
  Spin
} from 'antd';
import { 
  PlayCircleOutlined, 
  ReloadOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
  BarChartOutlined
} from '@ant-design/icons';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import ApiService from '../services/ApiService';

const { Option } = Select;

const MatchingEngine = () => {
  const [form] = Form.useForm();
  const [running, setRunning] = useState(false);
  const [customers, setCustomers] = useState([]);
  const [matchingHistory, setMatchingHistory] = useState([]);
  const [currentResults, setCurrentResults] = useState(null);
  const [progress, setProgress] = useState(0);
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    loadCustomers();
    loadMatchingHistory();
  }, []);

  const loadCustomers = async () => {
    try {
      const customerList = await ApiService.getCustomers();
      setCustomers(customerList);
    } catch (error) {
      console.error('Failed to load customers:', error);
    }
  };

  const loadMatchingHistory = async () => {
    try {
      const history = await ApiService.getMatchingHistory();
      setMatchingHistory(history.data || []);
    } catch (error) {
      console.error('Failed to load matching history:', error);
    }
  };

  const handleRunMatching = async (values) => {
    try {
      setRunning(true);
      setProgress(0);
      setLogs([]);
      setCurrentResults(null);

      // Add initial log
      addLog('Starting enhanced matching engine...', 'info');
      setProgress(10);

      const result = await ApiService.runMatching(
        values.customer_name, 
        values.po_number,
        {
          options: values.options || {}
        }
      );

      // Simulate progress updates (in real implementation, this would come from backend)
      const progressSteps = [
        { progress: 25, message: 'Loading orders and shipments...' },
        { progress: 40, message: 'Running Layer 0: Perfect matching...' },
        { progress: 55, message: 'Running Layer 1: Style+Color exact...' },
        { progress: 70, message: 'Running Layer 2: Fuzzy matching...' },
        { progress: 85, message: 'Running Layer 3: Quantity resolution...' },
        { progress: 95, message: 'Storing results and updating movement table...' },
        { progress: 100, message: 'Matching completed successfully!' }
      ];

      for (const step of progressSteps) {
        await new Promise(resolve => setTimeout(resolve, 500));
        setProgress(step.progress);
        addLog(step.message, 'info');
      }

      setCurrentResults(result);
      
      if (result.status === 'SUCCESS') {
        message.success(`Matching completed! ${result.total_matches}/${result.total_shipments} matches (${result.match_rate.toFixed(1)}%)`);
        addLog(`🎉 Success: ${result.total_matches} matches found`, 'success');
        addLog(`📊 Match rate: ${result.match_rate.toFixed(1)}%`, 'success');
        
        // Log layer breakdown
        Object.entries(result.layer_summary).forEach(([layer, count]) => {
          if (count > 0) {
            addLog(`${layer}: ${count} matches`, 'info');
          }
        });
      } else {
        message.warning('Matching completed with warnings');
        addLog('⚠️ Matching completed with warnings', 'warning');
      }

      loadMatchingHistory();

    } catch (error) {
      message.error('Matching failed: ' + error.message);
      addLog('❌ Error: ' + error.message, 'error');
    } finally {
      setRunning(false);
    }
  };

  const addLog = (message, type = 'info') => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs(prev => [...prev, { timestamp, message, type }]);
  };

  const getLayerColor = (layer) => {
    switch (layer) {
      case 'LAYER_0': return '#52c41a'; // Green
      case 'LAYER_1': return '#1890ff'; // Blue
      case 'LAYER_2': return '#faad14'; // Orange
      case 'LAYER_3': return '#ff4d4f'; // Red
      default: return '#666';
    }
  };

  const historyColumns = [
    {
      title: 'Session ID',
      dataIndex: 'session_id',
      key: 'session_id',
      width: 100,
      render: (id) => id?.slice(-8) || 'N/A'
    },
    {
      title: 'Customer',
      dataIndex: 'customer_name',
      key: 'customer_name',
      width: 120
    },
    {
      title: 'PO',
      dataIndex: 'po_number',
      key: 'po_number',
      width: 100
    },
    {
      title: 'Total Shipments',
      dataIndex: 'total_shipments',
      key: 'total_shipments',
      width: 120
    },
    {
      title: 'Matches',
      dataIndex: 'total_matches',
      key: 'total_matches',
      width: 100
    },
    {
      title: 'Match Rate',
      dataIndex: 'match_rate',
      key: 'match_rate',
      width: 100,
      render: (rate) => `${rate?.toFixed(1)}%`
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status) => (
        <Tag color={status === 'SUCCESS' ? 'green' : status === 'ERROR' ? 'red' : 'orange'}>
          {status}
        </Tag>
      )
    },
    {
      title: 'Started',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 150,
      render: (date) => new Date(date).toLocaleString()
    }
  ];

  const sampleMatchesColumns = [
    {
      title: 'Shipment',
      dataIndex: 'shipment_id',
      key: 'shipment_id',
      width: 100
    },
    {
      title: 'Order',
      dataIndex: 'order_id',
      key: 'order_id',
      width: 100
    },
    {
      title: 'Layer',
      dataIndex: 'match_layer',
      key: 'match_layer',
      width: 80,
      render: (layer) => (
        <Tag color={getLayerColor(layer)}>
          {layer?.replace('LAYER_', 'L')}
        </Tag>
      )
    },
    {
      title: 'Confidence',
      dataIndex: 'confidence',
      key: 'confidence',
      width: 100,
      render: (confidence) => `${(confidence * 100).toFixed(1)}%`
    },
    {
      title: 'Style',
      dataIndex: 'style_code',
      key: 'style_code',
      width: 120,
      ellipsis: true
    },
    {
      title: 'Color',
      dataIndex: 'color_description',
      key: 'color_description',
      width: 120,
      ellipsis: true
    },
    {
      title: 'Ship Qty',
      dataIndex: 'shipment_quantity',
      key: 'shipment_quantity',
      width: 80
    },
    {
      title: 'Order Qty',
      dataIndex: 'order_quantity',
      key: 'order_quantity',
      width: 80
    },
    {
      title: 'Variance',
      dataIndex: 'quantity_variance',
      key: 'quantity_variance',
      width: 80,
      render: (variance) => (
        <span style={{ color: variance === 0 ? '#52c41a' : Math.abs(variance) < 5 ? '#faad14' : '#ff4d4f' }}>
          {variance > 0 ? '+' : ''}{variance}
        </span>
      )
    }
  ];

  return (
    <div>
      {/* Matching Form */}
      <Card title="🚀 Run Enhanced Matching" style={{ marginBottom: '24px' }}>
        <Form
          form={form}
          layout="vertical"
          onFinish={handleRunMatching}
          disabled={running}
        >
          <Row gutter={[16, 16]}>
            <Col xs={24} sm={12} md={8}>
              <Form.Item
                label="Customer"
                name="customer_name"
                rules={[{ required: true, message: 'Please select a customer' }]}
              >
                <Select
                  placeholder="Select customer"
                  showSearch
                  filterOption={(input, option) =>
                    option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
                  }
                >
                  {customers.map(customer => (
                    <Option key={customer} value={customer}>{customer}</Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>
            
            <Col xs={24} sm={12} md={8}>
              <Form.Item
                label="PO Number (Optional)"
                name="po_number"
              >
                <Input placeholder="e.g., 4755" />
              </Form.Item>
            </Col>
            
            <Col xs={24} sm={12} md={8}>
              <Form.Item label=" " style={{ marginBottom: 0 }}>
                <Space>
                  <Button 
                    type="primary" 
                    htmlType="submit" 
                    icon={<PlayCircleOutlined />}
                    loading={running}
                    size="large"
                  >
                    Run Matching
                  </Button>
                  <Button 
                    icon={<ReloadOutlined />} 
                    onClick={loadMatchingHistory}
                  >
                    Refresh
                  </Button>
                </Space>
              </Form.Item>
            </Col>
          </Row>
        </Form>

        {/* Progress */}
        {running && (
          <div style={{ marginTop: '24px' }}>
            <Progress 
              percent={progress} 
              status={progress === 100 ? 'success' : 'active'}
              strokeColor={{
                '0%': '#108ee9',
                '100%': '#87d068',
              }}
            />
          </div>
        )}
      </Card>

      {/* Layer Information */}
      <Card title="🎯 4-Layer Matching System" style={{ marginBottom: '24px' }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center', padding: '16px', border: '1px solid #d9d9d9', borderRadius: '6px' }}>
              <div style={{ fontSize: '24px', marginBottom: '8px' }}>🎯</div>
              <div style={{ fontWeight: 'bold', color: '#52c41a' }}>Layer 0: Perfect</div>
              <div style={{ fontSize: '12px', color: '#666' }}>
                Exact style + color + delivery<br/>
                100% confidence, auto-approved
              </div>
            </div>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center', padding: '16px', border: '1px solid #d9d9d9', borderRadius: '6px' }}>
              <div style={{ fontSize: '24px', marginBottom: '8px' }}>🔵</div>
              <div style={{ fontWeight: 'bold', color: '#1890ff' }}>Layer 1: Style+Color</div>
              <div style={{ fontSize: '12px', color: '#666' }}>
                Exact style + color match<br/>
                85-95% confidence
              </div>
            </div>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center', padding: '16px', border: '1px solid #d9d9d9', borderRadius: '6px' }}>
              <div style={{ fontSize: '24px', marginBottom: '8px' }}>🟡</div>
              <div style={{ fontWeight: 'bold', color: '#faad14' }}>Layer 2: Fuzzy</div>
              <div style={{ fontSize: '12px', color: '#666' }}>
                Fuzzy style + color matching<br/>
                60-85% confidence
              </div>
            </div>
          </Col>
          
          <Col xs={24} sm={12} md={6}>
            <div style={{ textAlign: 'center', padding: '16px', border: '1px solid #d9d9d9', borderRadius: '6px' }}>
              <div style={{ fontSize: '24px', marginBottom: '8px' }}>🔴</div>
              <div style={{ fontWeight: 'bold', color: '#ff4d4f' }}>Layer 3: Resolution</div>
              <div style={{ fontSize: '12px', color: '#666' }}>
                Quantity & split resolution<br/>
                Variable confidence
              </div>
            </div>
          </Col>
        </Row>
      </Card>

      {/* Current Results */}
      {currentResults && (
        <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
          <Col xs={24} lg={12}>
            <Card title="📊 Latest Results">
              <Row gutter={[16, 16]}>
                <Col xs={12} sm={6}>
                  <Statistic
                    title="Total Shipments"
                    value={currentResults.total_shipments}
                    prefix={<BarChartOutlined />}
                  />
                </Col>
                <Col xs={12} sm={6}>
                  <Statistic
                    title="Matches Found"
                    value={currentResults.total_matches}
                    prefix={<CheckCircleOutlined />}
                    valueStyle={{ color: '#52c41a' }}
                  />
                </Col>
                <Col xs={12} sm={6}>
                  <Statistic
                    title="Match Rate"
                    value={currentResults.match_rate}
                    suffix="%"
                    precision={1}
                    valueStyle={{ color: currentResults.match_rate > 90 ? '#52c41a' : '#faad14' }}
                  />
                </Col>
                <Col xs={12} sm={6}>
                  <Statistic
                    title="Unmatched"
                    value={currentResults.unmatched_shipments}
                    prefix={<ClockCircleOutlined />}
                    valueStyle={{ color: currentResults.unmatched_shipments > 0 ? '#ff4d4f' : '#52c41a' }}
                  />
                </Col>
              </Row>

              {/* Layer Distribution Chart */}
              {currentResults.layer_summary && (
                <div style={{ marginTop: '24px' }}>
                  <h4>Layer Distribution</h4>
                  <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={Object.entries(currentResults.layer_summary).map(([layer, count]) => ({ layer, count }))}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="layer" />
                      <YAxis />
                      <Tooltip />
                      <Bar dataKey="count" fill="#1890ff" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}
            </Card>
          </Col>

          <Col xs={24} lg={12}>
            <Card title="📋 Execution Log">
              <div style={{ 
                height: '300px', 
                overflow: 'auto', 
                backgroundColor: '#001529', 
                padding: '12px', 
                borderRadius: '4px',
                fontFamily: 'monospace',
                fontSize: '12px'
              }}>
                {logs.map((log, index) => (
                  <div key={index} style={{ 
                    color: log.type === 'error' ? '#ff4d4f' : 
                          log.type === 'success' ? '#52c41a' : 
                          log.type === 'warning' ? '#faad14' : '#ffffff',
                    marginBottom: '4px' 
                  }}>
                    [{log.timestamp}] {log.message}
                  </div>
                ))}
                {running && (
                  <div style={{ color: '#1890ff', marginBottom: '4px' }}>
                    <Spin size="small" /> Processing...
                  </div>
                )}
              </div>
            </Card>
          </Col>
        </Row>
      )}

      {/* Sample Matches */}
      {currentResults?.matches && currentResults.matches.length > 0 && (
        <Card title="📋 Sample Matches" style={{ marginBottom: '24px' }}>
          <Table
            columns={sampleMatchesColumns}
            dataSource={currentResults.matches.slice(0, 10)}
            pagination={false}
            size="small"
            rowKey="shipment_id"
            scroll={{ x: 900 }}
          />
          {currentResults.matches.length > 10 && (
            <Alert
              message={`Showing 10 of ${currentResults.matches.length} total matches`}
              description="View all matches in the HITL Review Center or Queue Manager."
              type="info"
              showIcon
              style={{ marginTop: '16px' }}
            />
          )}
        </Card>
      )}

      {/* Matching History */}
      <Card title="📈 Matching History">
        <Table
          columns={historyColumns}
          dataSource={matchingHistory}
          pagination={{
            pageSize: 10,
            showSizeChanger: false,
            showQuickJumper: true
          }}
          size="small"
          rowKey="session_id"
          scroll={{ x: 800 }}
          locale={{
            emptyText: 'No matching history available'
          }}
        />
      </Card>
    </div>
  );
};

export default MatchingEngine;