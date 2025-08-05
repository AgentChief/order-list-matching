import React, { useState, useEffect } from 'react';
import { 
  Card, 
  Table, 
  Button, 
  Space, 
  Modal, 
  Form, 
  Input,
  Select,
  Tag,
  Descriptions,
  Row,
  Col,
  Statistic,
  message,
  Popconfirm,
  Badge,
  Divider,
  Alert
} from 'antd';
import { 
  CheckOutlined, 
  CloseOutlined,
  EyeOutlined,
  HistoryOutlined,
  ReloadOutlined,
  FilterOutlined
} from '@ant-design/icons';
import ApiService from '../services/ApiService';

const { Option } = Select;
const { TextArea } = Input;

const HitlReview = () => {
  const [loading, setLoading] = useState(false);
  const [reviewData, setReviewData] = useState([]);
  const [selectedRecord, setSelectedRecord] = useState(null);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [actionModalVisible, setActionModalVisible] = useState(false);
  const [actionType, setActionType] = useState(''); // 'approve' or 'reject'
  const [justification, setJustification] = useState('');
  const [filters, setFilters] = useState({});
  const [reviewStats, setReviewStats] = useState({
    total: 0,
    highPriority: 0,
    quantityIssues: 0,
    deliveryIssues: 0
  });

  const [form] = Form.useForm();

  useEffect(() => {
    loadReviewData();
  }, [filters]);

  const loadReviewData = async () => {
    try {
      setLoading(true);
      const response = await ApiService.getHitlQueue(filters);
      setReviewData(response.data || []);
      
      // Calculate review stats
      const stats = {
        total: response.data?.length || 0,
        highPriority: response.data?.filter(item => 
          item.match_confidence < 0.6 || Math.abs(item.quantity_difference_percent) > 20
        ).length || 0,
        quantityIssues: response.data?.filter(item => 
          item.review_reason === 'Quantity Review'
        ).length || 0,
        deliveryIssues: response.data?.filter(item => 
          item.review_reason === 'Delivery Review'
        ).length || 0,
      };
      setReviewStats(stats);
      
    } catch (error) {
      message.error('Failed to load review data');
      console.error('Review data loading error:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleViewDetail = (record) => {
    setSelectedRecord(record);
    setDetailModalVisible(true);
  };

  const handleAction = (record, action) => {
    setSelectedRecord(record);
    setActionType(action);
    setJustification('');
    setActionModalVisible(true);
  };

  const handleSubmitAction = async () => {
    if (!selectedRecord || !actionType) return;

    try {
      if (actionType === 'approve') {
        await ApiService.approveMatch(selectedRecord.id, justification);
        message.success('Match approved successfully');
      } else {
        await ApiService.rejectMatch(selectedRecord.id, justification);
        message.success('Match rejected successfully');
      }
      
      setActionModalVisible(false);
      setJustification('');
      loadReviewData();
      
    } catch (error) {
      message.error(`Failed to ${actionType} match`);
    }
  };

  const getPriorityLevel = (record) => {
    const confidence = record.match_confidence;
    const qtyDiff = Math.abs(record.quantity_difference_percent || 0);
    
    if (confidence < 0.5 || qtyDiff > 30) return 'high';
    if (confidence < 0.7 || qtyDiff > 15) return 'medium';
    return 'low';
  };

  const getPriorityColor = (priority) => {
    switch (priority) {
      case 'high': return '#ff4d4f';
      case 'medium': return '#faad14';
      case 'low': return '#52c41a';
      default: return '#1890ff';
    }
  };

  const getMatchTypeIcon = (layer) => {
    switch (layer) {
      case 'LAYER_0': return '🎯'; // Perfect match
      case 'LAYER_1': return '🔵'; // Style+Color exact
      case 'LAYER_2': return '🟡'; // Fuzzy match
      case 'LAYER_3': return '🔴'; // Quantity resolution
      default: return '❓';
    }
  };

  const columns = [
    {
      title: 'Priority',
      key: 'priority',
      width: 80,
      render: (_, record) => {
        const priority = getPriorityLevel(record);
        return (
          <Badge 
            color={getPriorityColor(priority)} 
            text={priority.toUpperCase()}
          />
        );
      },
      sorter: (a, b) => {
        const aPriority = getPriorityLevel(a);
        const bPriority = getPriorityLevel(b);
        const priorityOrder = { high: 3, medium: 2, low: 1 };
        return priorityOrder[bPriority] - priorityOrder[aPriority];
      }
    },
    {
      title: 'Match Info',
      key: 'match_info',
      width: 150,
      render: (_, record) => (
        <div>
          <div style={{ fontWeight: 'bold' }}>
            {getMatchTypeIcon(record.match_layer)} {record.match_layer?.replace('LAYER_', 'L')}
          </div>
          <div style={{ fontSize: '12px', color: '#666' }}>
            Confidence: {(record.match_confidence * 100).toFixed(1)}%
          </div>
        </div>
      )
    },
    {
      title: 'Customer & PO',
      key: 'customer_po',
      width: 150,
      render: (_, record) => (
        <div>
          <div style={{ fontWeight: 'bold' }}>{record.customer_name}</div>
          <div style={{ fontSize: '12px', color: '#666' }}>PO: {record.po_number}</div>
        </div>
      )
    },
    {
      title: 'Order/Shipment',
      key: 'order_shipment',
      width: 120,
      render: (_, record) => (
        <div>
          <div>Order: {record.order_id}</div>
          <div>Ship: {record.shipment_id}</div>
        </div>
      )
    },
    {
      title: 'Style Match',
      dataIndex: 'style_match',
      key: 'style_match',
      width: 100,
      render: (match) => (
        <Tag color={match === 'EXACT' ? 'green' : match === 'FUZZY' ? 'orange' : 'red'}>
          {match}
        </Tag>
      )
    },
    {
      title: 'Color Match',
      dataIndex: 'color_match',
      key: 'color_match',
      width: 100,
      render: (match) => (
        <Tag color={match === 'EXACT' ? 'green' : match === 'FUZZY' ? 'orange' : 'red'}>
          {match}
        </Tag>
      )
    },
    {
      title: 'Delivery Match',
      dataIndex: 'delivery_match',
      key: 'delivery_match',
      width: 110,
      render: (match) => (
        <Tag color={match === 'EXACT' ? 'green' : match === 'SIMILAR' ? 'blue' : 'red'}>
          {match}
        </Tag>
      )
    },
    {
      title: 'Qty Check',
      key: 'quantity_check',
      width: 100,
      render: (_, record) => (
        <div>
          <Tag color={record.quantity_check_result === 'PASS' ? 'green' : 'red'}>
            {record.quantity_check_result}
          </Tag>
          <div style={{ fontSize: '11px', color: '#666' }}>
            {record.quantity_difference_percent?.toFixed(1)}%
          </div>
        </div>
      )
    },
    {
      title: 'Review Reason',
      dataIndex: 'review_reason',
      key: 'review_reason',
      width: 120,
      render: (reason) => {
        const color = reason === 'Quantity Review' ? 'red' :
                    reason === 'Delivery Review' ? 'orange' :
                    reason === 'Low Confidence' ? 'blue' : 'default';
        return <Tag color={color}>{reason}</Tag>;
      }
    },
    {
      title: 'Created',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 120,
      render: (date) => new Date(date).toLocaleDateString(),
      sorter: (a, b) => new Date(a.created_at) - new Date(b.created_at)
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 150,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Button 
            type="text" 
            size="small"
            icon={<EyeOutlined />} 
            onClick={() => handleViewDetail(record)}
            title="View Details"
          />
          <Button 
            type="text" 
            size="small"
            icon={<CheckOutlined />} 
            onClick={() => handleAction(record, 'approve')}
            style={{ color: '#52c41a' }}
            title="Approve"
          />
          <Button 
            type="text" 
            size="small"
            icon={<CloseOutlined />} 
            onClick={() => handleAction(record, 'reject')}
            danger
            title="Reject"
          />
        </Space>
      )
    }
  ];

  return (
    <div>
      {/* Review Statistics */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Total Reviews"
              value={reviewStats.total}
              valueStyle={{ color: reviewStats.total > 0 ? '#1890ff' : '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="High Priority"
              value={reviewStats.highPriority}
              valueStyle={{ color: reviewStats.highPriority > 0 ? '#ff4d4f' : '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Quantity Issues"
              value={reviewStats.quantityIssues}
              valueStyle={{ color: reviewStats.quantityIssues > 0 ? '#ff4d4f' : '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Delivery Issues"
              value={reviewStats.deliveryIssues}
              valueStyle={{ color: reviewStats.deliveryIssues > 0 ? '#faad14' : '#52c41a' }}
            />
          </Card>
        </Col>
      </Row>

      {/* Status Alert */}
      {reviewStats.total === 0 ? (
        <Alert
          message="🎉 Excellent! No items requiring human review."
          description="All matches are high confidence and pass quality checks."
          type="success"
          showIcon
          style={{ marginBottom: '24px' }}
        />
      ) : reviewStats.highPriority > 0 && (
        <Alert
          message={`⚠️ ${reviewStats.highPriority} high priority items need attention`}
          description="These items have very low confidence or significant quantity discrepancies."
          type="warning"
          showIcon
          style={{ marginBottom: '24px' }}
        />
      )}

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
              {[...new Set(reviewData.map(item => item.customer_name))].map(customer => (
                <Option key={customer} value={customer}>{customer}</Option>
              ))}
            </Select>
          </Col>
          
          <Col xs={24} sm={8} md={6}>
            <Select
              style={{ width: '100%' }}
              placeholder="Filter by Priority"
              allowClear
              value={filters.priority}
              onChange={(value) => {
                // This would need backend support for priority filtering
                setFilters(prev => ({ ...prev, priority: value || '' }));
              }}
            >
              <Option value="high">High Priority</Option>
              <Option value="medium">Medium Priority</Option>
              <Option value="low">Low Priority</Option>
            </Select>
          </Col>

          <Col xs={24} sm={8} md={12} style={{ textAlign: 'right' }}>
            <Space>
              <Button 
                icon={<ReloadOutlined />} 
                onClick={loadReviewData}
                loading={loading}
              >
                Refresh
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Review Table */}
      <Card>
        <Table
          columns={columns}
          dataSource={reviewData}
          loading={loading}
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => 
              `${range[0]}-${range[1]} of ${total} reviews`,
          }}
          scroll={{ x: 1400 }}
          size="small"
          rowKey="id"
          rowClassName={(record) => {
            const priority = getPriorityLevel(record);
            return priority === 'high' ? 'high-priority-row' : '';
          }}
        />
      </Card>

      {/* Detail Modal */}
      <Modal
        title="Match Review Details"
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        width={800}
        footer={[
          <Button key="close" onClick={() => setDetailModalVisible(false)}>
            Close
          </Button>,
          <Button 
            key="approve" 
            type="primary" 
            icon={<CheckOutlined />}
            onClick={() => {
              setDetailModalVisible(false);
              handleAction(selectedRecord, 'approve');
            }}
          >
            Approve
          </Button>,
          <Button 
            key="reject" 
            danger 
            icon={<CloseOutlined />}
            onClick={() => {
              setDetailModalVisible(false);
              handleAction(selectedRecord, 'reject');
            }}
          >
            Reject
          </Button>
        ]}
      >
        {selectedRecord && (
          <div>
            <Descriptions title="Match Information" column={2} bordered size="small">
              <Descriptions.Item label="Match Layer">
                {getMatchTypeIcon(selectedRecord.match_layer)} {selectedRecord.match_layer}
              </Descriptions.Item>
              <Descriptions.Item label="Confidence">
                <span style={{ color: getPriorityColor(getPriorityLevel(selectedRecord)) }}>
                  {(selectedRecord.match_confidence * 100).toFixed(1)}%
                </span>
              </Descriptions.Item>
              <Descriptions.Item label="Customer">{selectedRecord.customer_name}</Descriptions.Item>
              <Descriptions.Item label="PO Number">{selectedRecord.po_number}</Descriptions.Item>
              <Descriptions.Item label="Order ID">{selectedRecord.order_id}</Descriptions.Item>
              <Descriptions.Item label="Shipment ID">{selectedRecord.shipment_id}</Descriptions.Item>
            </Descriptions>

            <Divider />

            <Descriptions title="Match Quality" column={3} bordered size="small">
              <Descriptions.Item label="Style Match">
                <Tag color={selectedRecord.style_match === 'EXACT' ? 'green' : 'orange'}>
                  {selectedRecord.style_match}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Color Match">
                <Tag color={selectedRecord.color_match === 'EXACT' ? 'green' : 'orange'}>
                  {selectedRecord.color_match}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Delivery Match">
                <Tag color={selectedRecord.delivery_match === 'EXACT' ? 'green' : 'blue'}>
                  {selectedRecord.delivery_match}
                </Tag>
              </Descriptions.Item>
            </Descriptions>

            <Divider />

            <Descriptions title="Quantity Analysis" column={2} bordered size="small">
              <Descriptions.Item label="Quantity Check">
                <Tag color={selectedRecord.quantity_check_result === 'PASS' ? 'green' : 'red'}>
                  {selectedRecord.quantity_check_result}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Difference %">
                <span style={{ 
                  color: Math.abs(selectedRecord.quantity_difference_percent) > 10 ? '#ff4d4f' : '#52c41a' 
                }}>
                  {selectedRecord.quantity_difference_percent?.toFixed(1)}%
                </span>
              </Descriptions.Item>
            </Descriptions>

            <Divider />

            <Descriptions title="Review Information" column={1} bordered size="small">
              <Descriptions.Item label="Review Reason">
                <Tag color={
                  selectedRecord.review_reason === 'Quantity Review' ? 'red' :
                  selectedRecord.review_reason === 'Delivery Review' ? 'orange' :
                  selectedRecord.review_reason === 'Low Confidence' ? 'blue' : 'default'
                }>
                  {selectedRecord.review_reason}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Created At">
                {new Date(selectedRecord.created_at).toLocaleString()}
              </Descriptions.Item>
            </Descriptions>
          </div>
        )}
      </Modal>

      {/* Action Modal */}
      <Modal
        title={`${actionType === 'approve' ? 'Approve' : 'Reject'} Match`}
        open={actionModalVisible}
        onCancel={() => setActionModalVisible(false)}
        onOk={handleSubmitAction}
        okText={actionType === 'approve' ? 'Approve' : 'Reject'}
        okButtonProps={{ 
          danger: actionType === 'reject',
          type: actionType === 'approve' ? 'primary' : 'default'
        }}
      >
        <div style={{ marginBottom: '16px' }}>
          <strong>
            {actionType === 'approve' ? 'Approving' : 'Rejecting'} match for:
          </strong>
          <br />
          Customer: {selectedRecord?.customer_name}
          <br />
          PO: {selectedRecord?.po_number}
          <br />
          Order: {selectedRecord?.order_id} → Shipment: {selectedRecord?.shipment_id}
        </div>
        
        <Form layout="vertical">
          <Form.Item label="Justification (Optional)">
            <TextArea
              rows={3}
              value={justification}
              onChange={(e) => setJustification(e.target.value)}
              placeholder={`Reason for ${actionType === 'approve' ? 'approving' : 'rejecting'} this match...`}
            />
          </Form.Item>
        </Form>
      </Modal>

      <style jsx>{`
        .high-priority-row {
          background-color: #fff2f0 !important;
        }
        .high-priority-row:hover {
          background-color: #ffece6 !important;
        }
      `}</style>
    </div>
  );
};

export default HitlReview;