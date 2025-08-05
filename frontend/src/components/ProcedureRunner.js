import React, { useState, useEffect } from 'react';
import { 
  Card, 
  Form, 
  Input, 
  Button, 
  Select, 
  Space, 
  message, 
  Table,
  Row,
  Col,
  Alert,
  Tag,
  Descriptions,
  Modal,
  Spin,
  InputNumber
} from 'antd';
import { 
  PlayCircleOutlined, 
  ReloadOutlined,
  HistoryOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined
} from '@ant-design/icons';
import ApiService from '../services/ApiService';

const { Option } = Select;
const { TextArea } = Input;

const ProcedureRunner = () => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [procedures, setProcedures] = useState([]);
  const [selectedProcedure, setSelectedProcedure] = useState('');
  const [procedureSchema, setProcedureSchema] = useState(null);
  const [executionHistory, setExecutionHistory] = useState([]);
  const [executionResult, setExecutionResult] = useState(null);
  const [detailModalVisible, setDetailModalVisible] = useState(false);

  useEffect(() => {
    loadAvailableProcedures();
    loadExecutionHistory();
  }, []);

  useEffect(() => {
    if (selectedProcedure) {
      loadProcedureSchema();
    }
  }, [selectedProcedure]);

  const loadAvailableProcedures = async () => {
    try {
      const procList = await ApiService.getAvailableProcedures();
      setProcedures(procList || []);
    } catch (error) {
      console.error('Failed to load procedures:', error);
      // Set default procedures for demo
      setProcedures([
        { name: 'sp_capture_order_placed', description: 'Capture order placement event' },
        { name: 'sp_capture_shipment_created', description: 'Capture shipment creation event' },
        { name: 'sp_capture_shipment_shipped', description: 'Capture shipment shipped event' },
        { name: 'sp_capture_reconciliation_event', description: 'Capture reconciliation event' },
        { name: 'sp_populate_movement_table_from_existing', description: 'Populate movement table from existing data' },
        { name: 'sp_update_cumulative_quantities', description: 'Update cumulative quantities' }
      ]);
    }
  };

  const loadProcedureSchema = async () => {
    try {
      const schema = await ApiService.getProcedureSchema(selectedProcedure);
      setProcedureSchema(schema);
    } catch (error) {
      console.error('Failed to load procedure schema:', error);
      // Set example schema based on procedure name
      setProcedureSchema(getExampleSchema(selectedProcedure));
    }
  };

  const loadExecutionHistory = async () => {
    try {
      // This would load from API in real implementation
      setExecutionHistory([
        {
          id: 1,
          procedure_name: 'sp_populate_movement_table_from_existing',
          parameters: { customer_filter: 'GREYSON' },
          status: 'SUCCESS',
          duration_ms: 2450,
          rows_affected: 1250,
          executed_at: new Date().toISOString(),
          executed_by: 'admin'
        },
        {
          id: 2,
          procedure_name: 'sp_update_cumulative_quantities',
          parameters: {},
          status: 'SUCCESS',
          duration_ms: 890,
          rows_affected: 456,
          executed_at: new Date(Date.now() - 3600000).toISOString(),
          executed_by: 'admin'
        }
      ]);
    } catch (error) {
      console.error('Failed to load execution history:', error);
    }
  };

  const getExampleSchema = (procedureName) => {
    const schemas = {
      'sp_capture_order_placed': {
        parameters: [
          { name: 'order_id', type: 'NVARCHAR(100)', required: true, description: 'Unique order identifier' },
          { name: 'customer_name', type: 'NVARCHAR(100)', required: true, description: 'Customer name' },
          { name: 'po_number', type: 'NVARCHAR(100)', required: true, description: 'Purchase order number' },
          { name: 'style_code', type: 'NVARCHAR(50)', required: true, description: 'Style code' },
          { name: 'color_description', type: 'NVARCHAR(100)', required: false, description: 'Color description' },
          { name: 'order_quantity', type: 'INT', required: true, description: 'Order quantity' },
          { name: 'unit_price', type: 'DECIMAL(10,2)', required: false, description: 'Unit price' }
        ]
      },
      'sp_populate_movement_table_from_existing': {
        parameters: [
          { name: 'customer_filter', type: 'NVARCHAR(100)', required: false, description: 'Customer name filter (optional)' },
          { name: 'batch_size', type: 'INT', required: false, description: 'Batch size for processing' }
        ]
      },
      'sp_update_cumulative_quantities': {
        parameters: [
          { name: 'customer_filter', type: 'NVARCHAR(100)', required: false, description: 'Customer name filter (optional)' }
        ]
      }
    };
    return schemas[procedureName] || { parameters: [] };
  };

  const handleExecuteProcedure = async (values) => {
    if (!selectedProcedure) {
      message.error('Please select a procedure to execute');
      return;
    }

    try {
      setLoading(true);
      setExecutionResult(null);

      // Prepare parameters
      const parameters = {};
      if (procedureSchema?.parameters) {
        procedureSchema.parameters.forEach(param => {
          if (values[param.name] !== undefined && values[param.name] !== '') {
            parameters[param.name] = values[param.name];
          }
        });
      }

      const result = await ApiService.executeProcedure(selectedProcedure, parameters);
      
      setExecutionResult({
        ...result,
        procedure_name: selectedProcedure,
        parameters,
        executed_at: new Date().toISOString(),
        status: result.success ? 'SUCCESS' : 'ERROR'
      });

      if (result.success) {
        message.success(`Procedure executed successfully! Affected ${result.rows_affected || 0} rows`);
      } else {
        message.error(`Procedure execution failed: ${result.error || 'Unknown error'}`);
      }

      loadExecutionHistory();
      form.resetFields();

    } catch (error) {
      message.error('Failed to execute procedure: ' + error.message);
      setExecutionResult({
        procedure_name: selectedProcedure,
        parameters: values,
        executed_at: new Date().toISOString(),
        status: 'ERROR',
        error: error.message
      });
    } finally {
      setLoading(false);
    }
  };

  const handleProcedureChange = (value) => {
    setSelectedProcedure(value);
    form.resetFields();
    setExecutionResult(null);
  };

  const renderParameterInput = (param) => {
    const baseProps = {
      placeholder: param.description,
      style: { width: '100%' }
    };

    if (param.type.includes('INT') || param.type.includes('DECIMAL')) {
      return <InputNumber {...baseProps} />;
    } else if (param.type.includes('BIT')) {
      return (
        <Select {...baseProps}>
          <Option value={true}>True</Option>
          <Option value={false}>False</Option>
        </Select>
      );
    } else if (param.name.toLowerCase().includes('date')) {
      return <Input type="datetime-local" {...baseProps} />;
    } else {
      return <Input {...baseProps} />;
    }
  };

  const historyColumns = [
    {
      title: 'Procedure',
      dataIndex: 'procedure_name',
      key: 'procedure_name',
      width: 200,
      ellipsis: true
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status) => (
        <Tag color={status === 'SUCCESS' ? 'green' : 'red'} icon={
          status === 'SUCCESS' ? <CheckCircleOutlined /> : <ExclamationCircleOutlined />
        }>
          {status}
        </Tag>
      )
    },
    {
      title: 'Duration',
      dataIndex: 'duration_ms',
      key: 'duration_ms',
      width: 100,
      render: (ms) => `${ms}ms`
    },
    {
      title: 'Rows Affected',
      dataIndex: 'rows_affected',
      key: 'rows_affected',
      width: 120
    },
    {
      title: 'Executed At',
      dataIndex: 'executed_at',
      key: 'executed_at',
      width: 150,
      render: (date) => new Date(date).toLocaleString()
    },
    {
      title: 'Executed By',
      dataIndex: 'executed_by',
      key: 'executed_by',
      width: 100
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 80,
      render: (_, record) => (
        <Button 
          type="text" 
          size="small"
          icon={<InfoCircleOutlined />} 
          onClick={() => {
            setExecutionResult(record);
            setDetailModalVisible(true);
          }}
        />
      )
    }
  ];

  return (
    <div>
      {/* Procedure Selection and Execution */}
      <Card title="🔧 Execute Stored Procedure" style={{ marginBottom: '24px' }}>
        <Form
          form={form}
          layout="vertical"
          onFinish={handleExecuteProcedure}
        >
          <Row gutter={[16, 16]}>
            <Col xs={24} md={12}>
              <Form.Item
                label="Select Procedure"
                name="procedure_name"
                rules={[{ required: true, message: 'Please select a procedure' }]}
              >
                <Select
                  placeholder="Choose a stored procedure"
                  onChange={handleProcedureChange}
                  showSearch
                  filterOption={(input, option) =>
                    option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
                  }
                >
                  {procedures.map(proc => (
                    <Option key={proc.name} value={proc.name}>
                      {proc.name}
                      {proc.description && (
                        <div style={{ fontSize: '12px', color: '#666' }}>
                          {proc.description}
                        </div>
                      )}
                    </Option>
                  ))}
                </Select>
              </Form.Item>
            </Col>

            <Col xs={24} md={12}>
              <Form.Item label=" " style={{ marginBottom: 0 }}>
                <Space>
                  <Button 
                    type="primary" 
                    htmlType="submit" 
                    icon={<PlayCircleOutlined />}
                    loading={loading}
                    disabled={!selectedProcedure}
                  >
                    Execute
                  </Button>
                  <Button 
                    icon={<ReloadOutlined />} 
                    onClick={loadExecutionHistory}
                  >
                    Refresh History
                  </Button>
                </Space>
              </Form.Item>
            </Col>
          </Row>

          {/* Parameter Inputs */}
          {procedureSchema?.parameters && procedureSchema.parameters.length > 0 && (
            <div>
              <h4>Parameters</h4>
              <Row gutter={[16, 16]}>
                {procedureSchema.parameters.map(param => (
                  <Col xs={24} sm={12} md={8} key={param.name}>
                    <Form.Item
                      label={`${param.name} ${param.required ? '*' : ''}`}
                      name={param.name}
                      rules={param.required ? [{ required: true, message: `${param.name} is required` }] : []}
                      help={`Type: ${param.type}${param.description ? ` - ${param.description}` : ''}`}
                    >
                      {renderParameterInput(param)}
                    </Form.Item>
                  </Col>
                ))}
              </Row>
            </div>
          )}
        </Form>
      </Card>

      {/* Execution Result */}
      {executionResult && (
        <Card title="📋 Execution Result" style={{ marginBottom: '24px' }}>
          <Alert
            message={executionResult.status === 'SUCCESS' ? 'Execution Successful' : 'Execution Failed'}
            description={
              executionResult.status === 'SUCCESS' 
                ? `Procedure completed successfully. ${executionResult.rows_affected || 0} rows affected.`
                : `Error: ${executionResult.error || 'Unknown error occurred'}`
            }
            type={executionResult.status === 'SUCCESS' ? 'success' : 'error'}
            showIcon
            style={{ marginBottom: '16px' }}
          />

          <Descriptions column={2} bordered size="small">
            <Descriptions.Item label="Procedure">{executionResult.procedure_name}</Descriptions.Item>
            <Descriptions.Item label="Status">
              <Tag color={executionResult.status === 'SUCCESS' ? 'green' : 'red'}>
                {executionResult.status}
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Executed At">
              {new Date(executionResult.executed_at).toLocaleString()}
            </Descriptions.Item>
            <Descriptions.Item label="Duration">
              {executionResult.duration_ms ? `${executionResult.duration_ms}ms` : 'N/A'}
            </Descriptions.Item>
            {executionResult.rows_affected !== undefined && (
              <Descriptions.Item label="Rows Affected" span={2}>
                {executionResult.rows_affected}
              </Descriptions.Item>
            )}
            {Object.keys(executionResult.parameters || {}).length > 0 && (
              <Descriptions.Item label="Parameters" span={2}>
                <pre style={{ margin: 0, fontSize: '12px' }}>
                  {JSON.stringify(executionResult.parameters, null, 2)}
                </pre>
              </Descriptions.Item>
            )}
            {executionResult.result_data && (
              <Descriptions.Item label="Result Data" span={2}>
                <pre style={{ margin: 0, fontSize: '12px', maxHeight: '200px', overflow: 'auto' }}>
                  {typeof executionResult.result_data === 'string' 
                    ? executionResult.result_data 
                    : JSON.stringify(executionResult.result_data, null, 2)}
                </pre>
              </Descriptions.Item>
            )}
          </Descriptions>
        </Card>
      )}

      {/* Common Procedures Quick Actions */}
      <Card title="⚡ Quick Actions" style={{ marginBottom: '24px' }}>
        <Row gutter={[16, 16]}>
          <Col xs={24} sm={12} md={8}>
            <Card size="small" title="Refresh Cache">
              <p style={{ fontSize: '12px', color: '#666', marginBottom: '12px' }}>
                Refresh the shipment summary cache for better performance
              </p>
              <Button 
                type="primary" 
                block 
                onClick={() => {
                  setSelectedProcedure('sp_refresh_cache');
                  form.setFieldsValue({ procedure_name: 'sp_refresh_cache' });
                }}
              >
                Refresh Cache
              </Button>
            </Card>
          </Col>

          <Col xs={24} sm={12} md={8}>
            <Card size="small" title="Update Quantities">
              <p style={{ fontSize: '12px', color: '#666', marginBottom: '12px' }}>
                Update cumulative shipped quantities
              </p>
              <Button 
                block 
                onClick={() => {
                  setSelectedProcedure('sp_update_cumulative_quantities');
                  form.setFieldsValue({ procedure_name: 'sp_update_cumulative_quantities' });
                }}
              >
                Update Quantities
              </Button>
            </Card>
          </Col>

          <Col xs={24} sm={12} md={8}>
            <Card size="small" title="Populate Movement Table">
              <p style={{ fontSize: '12px', color: '#666', marginBottom: '12px' }}>
                Populate movement table from existing data
              </p>
              <Button 
                block 
                onClick={() => {
                  setSelectedProcedure('sp_populate_movement_table_from_existing');
                  form.setFieldsValue({ procedure_name: 'sp_populate_movement_table_from_existing' });
                }}
              >
                Populate Data
              </Button>
            </Card>
          </Col>
        </Row>
      </Card>

      {/* Execution History */}
      <Card title="📈 Execution History">
        <Table
          columns={historyColumns}
          dataSource={executionHistory}
          pagination={{
            pageSize: 10,
            showSizeChanger: false,
            showQuickJumper: true
          }}
          size="small"
          rowKey="id"
          scroll={{ x: 900 }}
          locale={{
            emptyText: 'No execution history available'
          }}
        />
      </Card>

      {/* Detail Modal */}
      <Modal
        title="Execution Details"
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setDetailModalVisible(false)}>
            Close
          </Button>
        ]}
        width={600}
      >
        {executionResult && (
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item label="Procedure">{executionResult.procedure_name}</Descriptions.Item>
            <Descriptions.Item label="Status">
              <Tag color={executionResult.status === 'SUCCESS' ? 'green' : 'red'}>
                {executionResult.status}
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Executed At">
              {new Date(executionResult.executed_at).toLocaleString()}
            </Descriptions.Item>
            {executionResult.duration_ms && (
              <Descriptions.Item label="Duration">{executionResult.duration_ms}ms</Descriptions.Item>
            )}
            {executionResult.rows_affected !== undefined && (
              <Descriptions.Item label="Rows Affected">{executionResult.rows_affected}</Descriptions.Item>
            )}
            {executionResult.parameters && Object.keys(executionResult.parameters).length > 0 && (
              <Descriptions.Item label="Parameters">
                <pre style={{ margin: 0, fontSize: '12px' }}>
                  {JSON.stringify(executionResult.parameters, null, 2)}
                </pre>
              </Descriptions.Item>
            )}
            {executionResult.error && (
              <Descriptions.Item label="Error">
                <pre style={{ margin: 0, fontSize: '12px', color: '#ff4d4f' }}>
                  {executionResult.error}
                </pre>
              </Descriptions.Item>
            )}
          </Descriptions>
        )}
      </Modal>
    </div>
  );
};

export default ProcedureRunner;