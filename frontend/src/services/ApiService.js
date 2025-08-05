import axios from 'axios';

// API Base URL - will use backend API server
const API_BASE_URL = process.env.REACT_APP_BACKEND_URL || 'http://localhost:8001/api';

class ApiService {
  constructor() {
    this.client = axios.create({
      baseURL: API_BASE_URL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Request interceptor
    this.client.interceptors.request.use(
      (config) => {
        console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`);
        return config;
      },
      (error) => Promise.reject(error)
    );

    // Response interceptor
    this.client.interceptors.response.use(
      (response) => response.data,
      (error) => {
        console.error('API Error:', error.response?.data || error.message);
        throw error;
      }
    );
  }

  // System Status & Health
  async getSystemStatus() {
    try {
      const response = await this.client.get('/system/status');
      return {
        connected: true,
        lastUpdate: new Date().toISOString(),
        ...response
      };
    } catch (error) {
      return {
        connected: false,
        lastUpdate: null,
        totalMovements: 0,
        totalMatches: 0,
        error: error.message
      };
    }
  }

  // Dashboard Data
  async getDashboardData() {
    return this.client.get('/dashboard/overview');
  }

  async getSystemMetrics() {
    return this.client.get('/dashboard/metrics');
  }

  // Data Viewer Operations
  async getTableData(tableName, page = 1, limit = 100, filters = {}) {
    return this.client.get(`/data/${tableName}`, {
      params: { page, limit, ...filters }
    });
  }

  async getTableSchema(tableName) {
    return this.client.get(`/data/${tableName}/schema`);
  }

  async updateRecord(tableName, id, data) {
    return this.client.put(`/data/${tableName}/${id}`, data);
  }

  async deleteRecord(tableName, id) {
    return this.client.delete(`/data/${tableName}/${id}`);
  }

  async createRecord(tableName, data) {
    return this.client.post(`/data/${tableName}`, data);
  }

  // Queue Management
  async getHitlQueue(filters = {}) {
    return this.client.get('/queue/hitl', { params: filters });
  }

  async approveMatch(matchId, justification = '') {
    return this.client.post(`/queue/hitl/${matchId}/approve`, { justification });
  }

  async rejectMatch(matchId, justification = '') {
    return this.client.post(`/queue/hitl/${matchId}/reject`, { justification });
  }

  async bulkApprove(matchIds, justification = '') {
    return this.client.post('/queue/hitl/bulk-approve', { matchIds, justification });
  }

  async bulkReject(matchIds, justification = '') {
    return this.client.post('/queue/hitl/bulk-reject', { matchIds, justification });
  }

  async refreshQueue(queueType = 'hitl') {
    return this.client.post(`/queue/${queueType}/refresh`);
  }

  // Matching Engine
  async runMatching(customerName, poNumber = null, options = {}) {
    return this.client.post('/matching/run', {
      customer_name: customerName,
      po_number: poNumber,
      ...options
    });
  }

  async getMatchingHistory(limit = 50) {
    return this.client.get('/matching/history', { params: { limit } });
  }

  async getMatchingResults(sessionId) {
    return this.client.get(`/matching/results/${sessionId}`);
  }

  // Stored Procedures
  async executeProcedure(procedureName, parameters = {}) {
    return this.client.post('/procedures/execute', {
      procedure_name: procedureName,
      parameters
    });
  }

  async getAvailableProcedures() {
    return this.client.get('/procedures/list');
  }

  async getProcedureSchema(procedureName) {
    return this.client.get(`/procedures/${procedureName}/schema`);
  }

  // Movement Table Operations
  async getMovementData(filters = {}, page = 1, limit = 100) {
    return this.client.get('/movements', {
      params: { ...filters, page, limit }
    });
  }

  async getOrderMovements(orderId) {
    return this.client.get(`/movements/order/${orderId}`);
  }

  async getShipmentMovements(shipmentId) {
    return this.client.get(`/movements/shipment/${shipmentId}`);
  }

  async getOpenOrderBook(filters = {}) {
    return this.client.get('/movements/open-orders', { params: filters });
  }

  // Analytics
  async getLayerPerformance() {
    return this.client.get('/analytics/layer-performance');
  }

  async getCustomerPerformance() {
    return this.client.get('/analytics/customer-performance');
  }

  async getMatchingTrends(days = 30) {
    return this.client.get('/analytics/matching-trends', { params: { days } });
  }

  async getSystemPerformance() {
    return this.client.get('/analytics/system-performance');
  }

  // Utility Methods
  async executeQuery(query, parameters = []) {
    return this.client.post('/query/execute', {
      query,
      parameters
    });
  }

  async getCustomers() {
    return this.client.get('/customers');
  }

  async getAvailableTables() {
    return this.client.get('/schema/tables');
  }

  // Cache Management
  async refreshCache(cacheType = 'all') {
    return this.client.post(`/cache/refresh/${cacheType}`);
  }

  async getCacheStatus() {
    return this.client.get('/cache/status');
  }

  // Export/Import Operations
  async exportData(tableName, format = 'csv', filters = {}) {
    return this.client.get(`/export/${tableName}`, {
      params: { format, ...filters },
      responseType: 'blob'
    });
  }

  async importData(tableName, file) {
    const formData = new FormData();
    formData.append('file', file);
    
    return this.client.post(`/import/${tableName}`, formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
  }
}

export default new ApiService();