import React, { useState, useEffect } from 'react';
import { 
  Card, 
  Table, 
  Button, 
  Space, 
  Select, 
  Badge, 
  message, 
  Popconfirm,
  Row,
  Col,
  Statistic,
  Tag,
  Input,
  Dropdown,
  Menu
} from 'antd';
import { 
  ReloadOutlined, 
  CheckOutlined, 
  CloseOutlined,
  MoreOutlined,
  FilterOutlined,
  ExportOutlined
} from '@ant-design/icons';
import ApiService from '../services/ApiService';

const { Option } = Select;
const { Search } = Input;

const QueueManager = () => {
  const [loading, setLoading] = useState(false);
  const [queueData, setQueueData] = useState([]);
  const [selectedRowKeys, setSelectedRowKeys] = useState([]);
  const [filters, setFilters] = useState({
    customer: '',
    reviewReason: '',
    confidence: ''
  });
  const [queueStats, setQueueStats] = useState({
    total: 0,
    quantityReview: 0,
    deliveryReview: 0,
    lowConfidence: 0,
    generalReview: 0
  });

  useEffect(() => {
    loadQueueData();
  }, [filters]);

  const loadQueueData = async () => {
    try {
      setLoading(true);
      const response = await ApiService.getHitlQueue(filters);
      setQueueData(response.data || []);
      
      // Calculate stats
      const stats = {
        total: response.data?.length || 0,
        quantityReview: response.data?.filter(item => item.review_reason === 'Quantity Review').length || 0,
        deliveryReview: response.data?.filter(item => item.review_reason === 'Delivery Review').length || 0,
        lowConfidence: response.data?.filter(item => item.review_reason === 'Low Confidence').length || 0,
        generalReview: response.data?.filter(item => item.review_reason === 'General Review').length || 0,
      };
      setQueueStats(stats);
      
    } catch (error) {
      message.error('Failed to load queue data');
      console.error('Queue loading error:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleApprove = async (record) => {
    try {
      await ApiService.approveMatch(record.id, 'Manual approval from queue manager');
      message.success('Match approved successfully');
      loadQueueData();
    } catch (error) {
      message.error('Failed to approve match');
    }
  };

  const handleReject = async (record) => {
    try {
      await ApiService.rejectMatch(record.id, 'Manual rejection from queue manager');
      message.success('Match rejected successfully');
      loadQueueData();
    } catch (error) {
      message.error('Failed to reject match');
    }
  };

  const handleBulkApprove = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('Please select items to approve');
      return;
    }

    try {
      await ApiService.bulkApprove(selectedRowKeys, 'Bulk approval from queue manager');
      message.success(`${selectedRowKeys.length} matches approved successfully`);
      setSelectedRowKeys([]);
      loadQueueData();
    } catch (error) {
      message.error('Failed to bulk approve matches');
    }
  };

  const handleBulkReject = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('Please select items to reject');
      return;
    }

    try {
      await ApiService.bulkReject(selectedRowKeys, 'Bulk rejection from queue manager');
      message.success(`${selectedRowKeys.length} matches rejected successfully`);
      setSelectedRowKeys([]);
      loadQueueData();
    } catch (error) {
      message.error('Failed to bulk reject matches');
    }
  };

  const handleRefreshQueue = async () => {
    try {
      await ApiService.refreshQueue('hitl');
      message.success('Queue refreshed successfully');
      loadQueueData();
    } catch (error) {
      message.error('Failed to refresh queue');
    }
  };

  const handleExport = async () => {
    try {
      const blob = await ApiService.exportData('enhanced_matching_results', 'csv', filters);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'hitl_queue_export.csv';
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      message.success('Queue data exported successfully');
    } catch (error) {
      message.error('Failed to export queue data');
    }
  };

  const getReviewReasonColor = (reason) => {
    switch (reason) {
      case 'Quantity Review': return 'red';
      case 'Delivery Review': return 'orange';
      case 'Low Confidence': return 'blue';
      case 'General Review': return 'default';
      default: return 'default';
    }
  };

  const getConfidenceColor = (confidence) => {
    if (confidence >= 0.9) return '#52c41a';
    if (confidence >= 0.7) return '#faad14';
    return '#ff4d4f';
  };

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
      fixed: 'left'
    },
    {
      title: 'Customer',
      dataIndex: 'customer_name',
      key: 'customer_name',
      width: 120,
      ellipsis: true
    },
    {
      title: 'PO',
      dataIndex: 'po_number',
      key: 'po_number',
      width: 100
    },
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
        <Tag color={
          layer === 'LAYER_0' ? 'green' :
          layer === 'LAYER_1' ? 'blue' :
          layer === 'LAYER_2' ? 'orange' : 'red'
        }>
          {layer?.replace('LAYER_', 'L')}
        </Tag>
      )
    },
    {
      title: 'Confidence',
      dataIndex: 'match_confidence',
      key: 'match_confidence',
      width: 100,
      render: (confidence) => (
        <span style={{ color: getConfidenceColor(confidence) }}>
          {(confidence * 100).toFixed(1)}%
        </span>
      ),
      sorter: (a, b) => a.match_confidence - b.match_confidence
    },
    {
      title: 'Qty Diff %',
      dataIndex: 'quantity_difference_percent',
      key: 'quantity_difference_percent',
      width: 100,
      render: (diff) => (
        <span style={{ color: Math.abs(diff) > 10 ? '#ff4d4f' : '#52c41a' }}>
          {diff?.toFixed(1)}%
        </span>
      )
    },
    {
      title: 'Review Reason',
      dataIndex: 'review_reason',
      key: 'review_reason',
      width: 120,
      render: (reason) => (
        <Tag color={getReviewReasonColor(reason)}>
          {reason}
        </Tag>
      )
    },
    {
      title: 'Created',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 150,
      render: (date) => new Date(date).toLocaleString(),
      sorter: (a, b) => new Date(a.created_at) - new Date(b.created_at)
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Button 
            type="text" 
            size="small"
            icon={<CheckOutlined />} 
            onClick={() => handleApprove(record)}
            style={{ color: '#52c41a' }}
            title="Approve"
          />
          <Button 
            type="text" 
            size="small"
            icon={<CloseOutlined />} 
            onClick={() => handleReject(record)}
            danger
            title="Reject"
          />
        </Space>
      )
    }
  ];

  const rowSelection = {
    selectedRowKeys,
    onChange: setSelectedRowKeys,
    selections: [
      Table.SELECTION_ALL,
      Table.SELECTION_INVERT,
      Table.SELECTION_NONE,
      {
        key: 'low-confidence',
        text: 'Select Low Confidence',
        onSelect: (changeableRowKeys) => {
          const lowConfidenceKeys = queueData
            .filter(item => item.match_confidence < 0.7)
            .map(item => item.id);
          setSelectedRowKeys(lowConfidenceKeys);
        },
      },
    ],
  };

  const filterMenu = (
    <Menu>
      <Menu.Item key="clear-filters" onClick={() => setFilters({})}>
        Clear All Filters
      </Menu.Item>
      <Menu.Item key="quantity-issues" onClick={() => setFilters({reviewReason: 'Quantity Review'})}>
        Quantity Issues Only
      </Menu.Item>
      <Menu.Item key="delivery-issues" onClick={() => setFilters({reviewReason: 'Delivery Review'})}>
        Delivery Issues Only
      </Menu.Item>
      <Menu.Item key="low-confidence" onClick={() => setFilters({confidence: 'low'})}>
        Low Confidence Only
      </Menu.Item>
    </Menu>
  );

  return (
    <div>
      {/* Queue Statistics */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={12} sm={8} md={4}>
          <Card>
            <Statistic
              title="Total Queue"
              value={queueStats.total}
              valueStyle={{ color: queueStats.total > 0 ? '#1890ff' : '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={5}>
          <Card>
            <Statistic
              title="Quantity Issues"
              value={queueStats.quantityReview}
              valueStyle={{ color: queueStats.quantityReview > 0 ? '#ff4d4f' : '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={5}>
          <Card>
            <Statistic
              title="Delivery Issues"
              value={queueStats.deliveryReview}
              valueStyle={{ color: queueStats.deliveryReview > 0 ? '#faad14' : '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={5}>
          <Card>
            <Statistic
              title="Low Confidence"
              value={queueStats.lowConfidence}
              valueStyle={{ color: queueStats.lowConfidence > 0 ? '#1890ff' : '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={8} md={5}>
          <Card>
            <Statistic
              title="General Review"
              value={queueStats.generalReview}
              valueStyle={{ color: '#666' }}
            />
          </Card>
        </Col>
      </Row>

      {/* Controls */}
      <Card style={{ marginBottom: '16px' }}>
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} sm={8} md={6}>
            <Select
              style={{ width: '100%' }}
              placeholder="Filter by Customer"
              allowClear
              value={filters.customer}
              onChange={(value) => setFilters(prev => ({ ...prev, customer: value || '' }))}
            >
              {[...new Set(queueData.map(item => item.customer_name))].map(customer => (
                <Option key={customer} value={customer}>{customer}</Option>
              ))}
            </Select>
          </Col>
          
          <Col xs={24} sm={8} md={6}>
            <Select
              style={{ width: '100%' }}
              placeholder="Filter by Review Reason"
              allowClear
              value={filters.reviewReason}
              onChange={(value) => setFilters(prev => ({ ...prev, reviewReason: value || '' }))}
            >
              <Option value="Quantity Review">Quantity Review</Option>
              <Option value="Delivery Review">Delivery Review</Option>
              <Option value="Low Confidence">Low Confidence</Option>
              <Option value="General Review">General Review</Option>
            </Select>
          </Col>

          <Col xs={24} sm={8} md={12} style={{ textAlign: 'right' }}>
            <Space wrap>
              <Dropdown overlay={filterMenu} trigger={['click']}>
                <Button icon={<FilterOutlined />}>
                  Quick Filters
                </Button>
              </Dropdown>
              
              <Button 
                icon={<ExportOutlined />} 
                onClick={handleExport}
                disabled={!queueData.length}
              >
                Export
              </Button>
              
              <Button 
                icon={<ReloadOutlined />} 
                onClick={handleRefreshQueue}
                loading={loading}
              >
                Refresh
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Bulk Actions */}
      {selectedRowKeys.length > 0 && (
        <Card style={{ marginBottom: '16px', background: '#e6f7ff', border: '1px solid #91d5ff' }}>
          <Space>
            <span>
              <Badge count={selectedRowKeys.length} style={{ backgroundColor: '#1890ff' }}>
                Selected Items
              </Badge>
            </span>
            
            <Popconfirm
              title={`Are you sure you want to approve ${selectedRowKeys.length} selected items?`}
              onConfirm={handleBulkApprove}
              okText="Yes"
              cancelText="No"
            >
              <Button type="primary" icon={<CheckOutlined />}>
                Bulk Approve
              </Button>
            </Popconfirm>
            
            <Popconfirm
              title={`Are you sure you want to reject ${selectedRowKeys.length} selected items?`}
              onConfirm={handleBulkReject}
              okText="Yes"
              cancelText="No"
            >
              <Button danger icon={<CloseOutlined />}>
                Bulk Reject
              </Button>
            </Popconfirm>
            
            <Button onClick={() => setSelectedRowKeys([])}>
              Clear Selection
            </Button>
          </Space>
        </Card>
      )}

      {/* Queue Table */}
      <Card>
        <Table
          columns={columns}
          dataSource={queueData}
          loading={loading}
          rowSelection={rowSelection}
          pagination={{
            pageSize: 50,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => 
              `${range[0]}-${range[1]} of ${total} items`,
          }}
          scroll={{ x: 1200 }}
          size="small"
          rowKey="id"
          locale={{
            emptyText: queueStats.total === 0 ? 
              '🎉 Excellent! No items requiring review.' : 
              'No items match current filters'
          }}
        />
      </Card>
    </div>
  );
};

export default QueueManager;