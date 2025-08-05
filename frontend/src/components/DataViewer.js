import React, { useState, useEffect } from 'react';
import { 
  Table, 
  Select, 
  Input, 
  Button, 
  Space, 
  Modal, 
  Form, 
  message, 
  Popconfirm,
  Card,
  Row,
  Col,
  Drawer,
  Descriptions,
  Tag
} from 'antd';
import { 
  SearchOutlined, 
  PlusOutlined, 
  EditOutlined, 
  DeleteOutlined,
  ReloadOutlined,
  ExportOutlined,
  EyeOutlined
} from '@ant-design/icons';
import ApiService from '../services/ApiService';

const { Option } = Select;
const { Search } = Input;

const DataViewer = () => {
  const [loading, setLoading] = useState(false);
  const [tableData, setTableData] = useState([]);
  const [availableTables, setAvailableTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [tableSchema, setTableSchema] = useState([]);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 50,
    total: 0
  });
  const [filters, setFilters] = useState({});
  const [searchText, setSearchText] = useState('');
  
  // Modal states
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [detailDrawerVisible, setDetailDrawerVisible] = useState(false);
  const [currentRecord, setCurrentRecord] = useState(null);
  
  const [form] = Form.useForm();

  useEffect(() => {
    loadAvailableTables();
  }, []);

  useEffect(() => {
    if (selectedTable) {
      loadTableData();
      loadTableSchema();
    }
  }, [selectedTable, pagination.current, pagination.pageSize, filters]);

  const loadAvailableTables = async () => {
    try {
      const tables = await ApiService.getAvailableTables();
      setAvailableTables(tables);
      if (tables.length > 0 && !selectedTable) {
        setSelectedTable(tables[0].name);
      }
    } catch (error) {
      message.error('Failed to load available tables');
    }
  };

  const loadTableData = async () => {
    if (!selectedTable) return;
    
    try {
      setLoading(true);
      const response = await ApiService.getTableData(
        selectedTable, 
        pagination.current, 
        pagination.pageSize, 
        { ...filters, search: searchText }
      );
      
      setTableData(response.data || []);
      setPagination(prev => ({
        ...prev,
        total: response.total || 0
      }));
    } catch (error) {
      message.error('Failed to load table data');
      console.error('Table data loading error:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadTableSchema = async () => {
    if (!selectedTable) return;
    
    try {
      const schema = await ApiService.getTableSchema(selectedTable);
      setTableSchema(schema);
    } catch (error) {
      console.error('Failed to load table schema:', error);
    }
  };

  const handleTableChange = (value) => {
    setSelectedTable(value);
    setTableData([]);
    setPagination({ current: 1, pageSize: 50, total: 0 });
    setFilters({});
    setSearchText('');
  };

  const handlePaginationChange = (page, size) => {
    setPagination(prev => ({
      ...prev,
      current: page,
      pageSize: size
    }));
  };

  const handleSearch = (value) => {
    setSearchText(value);
    setPagination(prev => ({ ...prev, current: 1 }));
  };

  const handleEdit = (record) => {
    setCurrentRecord(record);
    form.setFieldsValue(record);
    setEditModalVisible(true);
  };

  const handleAdd = () => {
    setCurrentRecord(null);
    form.resetFields();
    setAddModalVisible(true);
  };

  const handleDelete = async (record) => {
    try {
      await ApiService.deleteRecord(selectedTable, record.id);
      message.success('Record deleted successfully');
      loadTableData();
    } catch (error) {
      message.error('Failed to delete record');
    }
  };

  const handleSave = async (values) => {
    try {
      if (currentRecord) {
        await ApiService.updateRecord(selectedTable, currentRecord.id, values);
        message.success('Record updated successfully');
        setEditModalVisible(false);
      } else {
        await ApiService.createRecord(selectedTable, values);
        message.success('Record created successfully');
        setAddModalVisible(false);
      }
      loadTableData();
      form.resetFields();
    } catch (error) {
      message.error(`Failed to ${currentRecord ? 'update' : 'create'} record`);
    }
  };

  const handleViewDetail = (record) => {
    setCurrentRecord(record);
    setDetailDrawerVisible(true);
  };

  const handleExport = async () => {
    try {
      const blob = await ApiService.exportData(selectedTable, 'csv', filters);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${selectedTable}_export.csv`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      message.success('Data exported successfully');
    } catch (error) {
      message.error('Failed to export data');
    }
  };

  // Generate table columns from schema
  const generateColumns = () => {
    if (!tableSchema.length) return [];

    const baseColumns = tableSchema.slice(0, 6).map(col => ({
      title: col.name,
      dataIndex: col.name,
      key: col.name,
      width: col.type === 'datetime' ? 150 : col.type === 'text' ? 200 : 120,
      ellipsis: true,
      render: (text, record) => {
        if (col.type === 'datetime' && text) {
          return new Date(text).toLocaleString();
        }
        if (col.type === 'boolean') {
          return text ? <Tag color="green">Yes</Tag> : <Tag>No</Tag>;
        }
        if (typeof text === 'string' && text.length > 50) {
          return text.substring(0, 50) + '...';
        }
        return text;
      }
    }));

    // Add actions column
    baseColumns.push({
      title: 'Actions',
      key: 'actions',
      width: 160,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Button 
            type="text" 
            icon={<EyeOutlined />} 
            onClick={() => handleViewDetail(record)}
            title="View Details"
          />
          <Button 
            type="text" 
            icon={<EditOutlined />} 
            onClick={() => handleEdit(record)}
            title="Edit"
          />
          <Popconfirm
            title="Are you sure you want to delete this record?"
            onConfirm={() => handleDelete(record)}
            okText="Yes"
            cancelText="No"
          >
            <Button 
              type="text" 
              danger 
              icon={<DeleteOutlined />}
              title="Delete"
            />
          </Popconfirm>
        </Space>
      )
    });

    return baseColumns;
  };

  // Generate form fields from schema
  const generateFormFields = () => {
    return tableSchema
      .filter(col => !col.isPrimaryKey && !col.isAutoIncrement)
      .map(col => (
        <Form.Item
          key={col.name}
          label={col.name}
          name={col.name}
          rules={col.required ? [{ required: true, message: `${col.name} is required` }] : []}
        >
          {col.type === 'boolean' ? (
            <Select>
              <Option value={true}>Yes</Option>
              <Option value={false}>No</Option>
            </Select>
          ) : col.type === 'text' ? (
            <Input.TextArea rows={3} />
          ) : col.type === 'datetime' ? (
            <Input type="datetime-local" />
          ) : (
            <Input />
          )}
        </Form.Item>
      ));
  };

  return (
    <div>
      {/* Header Controls */}
      <Card style={{ marginBottom: '16px' }}>
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} sm={8} md={6}>
            <Select
              style={{ width: '100%' }}
              placeholder="Select Table"
              value={selectedTable}
              onChange={handleTableChange}
              showSearch
              filterOption={(input, option) =>
                option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
              }
            >
              {availableTables.map(table => (
                <Option key={table.name} value={table.name}>
                  {table.displayName || table.name}
                </Option>
              ))}
            </Select>
          </Col>
          
          <Col xs={24} sm={8} md={6}>
            <Search
              placeholder="Search records..."
              allowClear
              onSearch={handleSearch}
              style={{ width: '100%' }}
            />
          </Col>
          
          <Col xs={24} sm={8} md={12} style={{ textAlign: 'right' }}>
            <Space wrap>
              <Button 
                icon={<ReloadOutlined />} 
                onClick={loadTableData}
                loading={loading}
              >
                Refresh
              </Button>
              <Button 
                icon={<ExportOutlined />} 
                onClick={handleExport}
                disabled={!selectedTable || !tableData.length}
              >
                Export
              </Button>
              <Button 
                type="primary" 
                icon={<PlusOutlined />} 
                onClick={handleAdd}
                disabled={!selectedTable}
              >
                Add Record
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Data Table */}
      <Card>
        <Table
          columns={generateColumns()}
          dataSource={tableData}
          loading={loading}
          pagination={{
            current: pagination.current,
            pageSize: pagination.pageSize,
            total: pagination.total,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => 
              `${range[0]}-${range[1]} of ${total} records`,
            onChange: handlePaginationChange,
            onShowSizeChange: handlePaginationChange,
          }}
          scroll={{ x: 1000 }}
          size="small"
          rowKey="id"
        />
      </Card>

      {/* Edit Modal */}
      <Modal
        title="Edit Record"
        open={editModalVisible}
        onCancel={() => setEditModalVisible(false)}
        onOk={() => form.submit()}
        width={600}
        maskClosable={false}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleSave}
        >
          {generateFormFields()}
        </Form>
      </Modal>

      {/* Add Modal */}
      <Modal
        title="Add New Record"
        open={addModalVisible}
        onCancel={() => setAddModalVisible(false)}
        onOk={() => form.submit()}
        width={600}
        maskClosable={false}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleSave}
        >
          {generateFormFields()}
        </Form>
      </Modal>

      {/* Detail Drawer */}
      <Drawer
        title="Record Details"
        placement="right"
        width={600}
        onClose={() => setDetailDrawerVisible(false)}
        open={detailDrawerVisible}
      >
        {currentRecord && (
          <Descriptions column={1} bordered size="small">
            {Object.entries(currentRecord).map(([key, value]) => (
              <Descriptions.Item label={key} key={key}>
                {typeof value === 'boolean' ? (
                  value ? <Tag color="green">Yes</Tag> : <Tag>No</Tag>
                ) : typeof value === 'string' && value.includes('T') && value.includes('Z') ? (
                  new Date(value).toLocaleString()
                ) : (
                  value?.toString() || 'N/A'
                )}
              </Descriptions.Item>
            ))}
          </Descriptions>
        )}
      </Drawer>
    </div>
  );
};

export default DataViewer;