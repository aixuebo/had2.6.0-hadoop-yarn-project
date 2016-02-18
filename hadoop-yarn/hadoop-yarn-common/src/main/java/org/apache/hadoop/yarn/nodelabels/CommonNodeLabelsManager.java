/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.nodelabels;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.event.StoreNewClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEvent;
import org.apache.hadoop.yarn.nodelabels.event.NodeLabelsStoreEventType;
import org.apache.hadoop.yarn.nodelabels.event.RemoveClusterNodeLabels;
import org.apache.hadoop.yarn.nodelabels.event.UpdateNodeToLabelsMappingsEvent;
import org.apache.hadoop.yarn.util.resource.Resources;

import com.google.common.collect.ImmutableSet;

/**
 * 公共的标签管理工具
 */
public class CommonNodeLabelsManager extends AbstractService {
  protected static final Log LOG = LogFactory.getLog(CommonNodeLabelsManager.class);
  //标签不允许大于最大字符
  private static final int MAX_LABEL_LENGTH = 255;
  //空标签集合
  public static final Set<String> EMPTY_STRING_SET = Collections.unmodifiableSet(new HashSet<String>(0));
      
  public static final String ANY = "*";
  public static final Set<String> ACCESS_ANY_LABEL_SET = ImmutableSet.of(ANY);
  //标签命名要符合正则表达式
  private static final Pattern LABEL_PATTERN = Pattern.compile("^[0-9a-zA-Z][0-9a-zA-Z-_]*");
  public static final int WILDCARD_PORT = 0;

  /**
   * If a user doesn't specify label of a queue or node, it belongs
   * DEFAULT_LABEL
   * 默认标签
   */
  public static final String NO_LABEL = "";

  protected Dispatcher dispatcher;

  /**
   * 每一个标签对应一个标签对象,该标签对象记录这该标签的使用资源
   * key是label字符串,value是Label对象
   */
  protected ConcurrentMap<String, Label> labelCollections = new ConcurrentHashMap<String, Label>();
      
  /**
   * 每一个host对应的host对象
   */
  protected ConcurrentMap<String, Host> nodeCollections = new ConcurrentHashMap<String, Host>();

  protected final ReadLock readLock;
  protected final WriteLock writeLock;

  protected NodeLabelsStore store;//标签存储器

  //记录每一个label标签所使用的资源情况
  protected static class Label {
    public Resource resource;

    protected Label() {
      this.resource = Resource.newInstance(0, 0);
    }
  }

  /**
   * A <code>Host</code> can have multiple <code>Node</code>s 
   * 以为一个host上可能存在多个node节点
   */
  protected static class Host {
    public Set<String> labels;//属于该host的标签集合
    public Map<NodeId, Node> nms;//该host上的node丢下映射
    
    protected Host() {
      labels = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
      nms = new ConcurrentHashMap<NodeId, Node>();
    }
    
    /**
     * 复制现有host对象
     */
    public Host copy() {
      Host c = new Host();
      c.labels = new HashSet<String>(labels);
      for (Entry<NodeId, Node> entry : nms.entrySet()) {
        c.nms.put(entry.getKey(), entry.getValue().copy());
      }
      return c;
    }
  }
  
  /**
   * 对应于host对象,因为一个host上可以有多个node节点,因此每一个节点对应一个该对象
   */
  protected static class Node {
    public Set<String> labels;//该节点对应的标签
    public Resource resource;//该节点使用的资源
    public boolean running;//该节点是否正在运行
    
    protected Node() {
      labels = null;
      resource = Resource.newInstance(0, 0);
      running = false;
    }
    
    public Node copy() {
      Node c = new Node();
      if (labels != null) {
        c.labels = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        c.labels.addAll(labels);
      } else {
        c.labels = null;
      }
      c.resource = Resources.clone(resource);
      c.running = running;
      return c;
    }
  }

  private final class ForwardingEventHandler implements
      EventHandler<NodeLabelsStoreEvent> {

    @Override
    public void handle(NodeLabelsStoreEvent event) {
      handleStoreEvent(event);
    }
  }
  
  // Dispatcher related code
  protected void handleStoreEvent(NodeLabelsStoreEvent event) {
    try {
      switch (event.getType()) {
      case ADD_LABELS://当添加完标签后,触发标签添加完成事件,会将这些标签存储起来
        StoreNewClusterNodeLabels storeNewClusterNodeLabelsEvent = (StoreNewClusterNodeLabels) event;
        store.storeNewClusterNodeLabels(storeNewClusterNodeLabelsEvent.getLabels());
        break;
      case REMOVE_LABELS://向文件中写入要删除的标签
        RemoveClusterNodeLabels removeClusterNodeLabelsEvent =
            (RemoveClusterNodeLabels) event;
        store.removeClusterNodeLabels(removeClusterNodeLabelsEvent.getLabels());
        break;
      case STORE_NODE_TO_LABELS:
        UpdateNodeToLabelsMappingsEvent updateNodeToLabelsMappingsEvent =
            (UpdateNodeToLabelsMappingsEvent) event;
        store.updateNodeToLabelsMappings(updateNodeToLabelsMappingsEvent
            .getNodeToLabels());
        break;
      }
    } catch (IOException e) {
      LOG.error("Failed to store label modification to storage");
      throw new YarnRuntimeException(e);
    }
  }

  public CommonNodeLabelsManager() {
    super(CommonNodeLabelsManager.class.getName());
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  // for UT purpose
  protected void initDispatcher(Configuration conf) {
    // create async handler
    dispatcher = new AsyncDispatcher();
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.init(conf);
    asyncDispatcher.setDrainEventsOnStop();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    initNodeLabelStore(conf);//初始化标签存储器
    
    //为空标签添加一个Label对象
    labelCollections.put(NO_LABEL, new Label());
  }

  /**
   * 初始化标签存储器
   */
  protected void initNodeLabelStore(Configuration conf) throws Exception {
    this.store = new FileSystemNodeLabelsStore(this);
    this.store.init(conf);
    this.store.recover();
  }

  // for UT purpose
  protected void startDispatcher() {
    // start dispatcher
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.start();
  }

  @Override
  protected void serviceStart() throws Exception {
    // init dispatcher only when service start, because recover will happen in
    // service init, we don't want to trigger any event handling at that time.
    initDispatcher(getConfig());

    if (null != dispatcher) {
      dispatcher.register(NodeLabelsStoreEventType.class,
          new ForwardingEventHandler());
    }
    
    startDispatcher();
  }
  
  // for UT purpose
  protected void stopDispatcher() {
    AsyncDispatcher asyncDispatcher = (AsyncDispatcher) dispatcher;
    asyncDispatcher.stop();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    // finalize store
    stopDispatcher();
    
    // only close store when we enabled store persistent
    if (null != store) {
      store.close();
    }
  }

  /**
   * Add multiple node labels to repository
   * 
   * @param labels
   *          new node labels added
   * 向集群中添加新标签
   */
  @SuppressWarnings("unchecked")
  public void addToCluserNodeLabels(Set<String> labels) throws IOException {
    if (null == labels || labels.isEmpty()) {
      return;
    }
    //新添加的标签
    Set<String> newLabels = new HashSet<String>();
    //格式化标签名称
    labels = normalizeLabels(labels);

    // do a check before actual adding them, will throw exception if any of them
    // doesn't meet label name requirement 检查标签名称是否合格
    for (String label : labels) {
      checkAndThrowLabelName(label);
    }

    for (String label : labels) {
      // shouldn't overwrite it to avoid changing the Label.resource
      if (this.labelCollections.get(label) == null) {
        this.labelCollections.put(label, new Label());
        newLabels.add(label);
      }
    }
    //发事件,将新增加的标签发事件通知
    if (null != dispatcher && !newLabels.isEmpty()) {
      dispatcher.getEventHandler().handle(new StoreNewClusterNodeLabels(newLabels));
    }

    LOG.info("Add labels: [" + StringUtils.join(labels.iterator(), ",") + "]");
  }
  
  /**
   * 检查每一个Node上的全部标签集合是否存在,如果不存在则抛异常
   */
  protected void checkAddLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode) throws IOException {
    if (null == addedLabelsToNode || addedLabelsToNode.isEmpty()) {//不用检查
      return;
    }

    // check all labels being added existed获取已知标签集合
    Set<String> knownLabels = labelCollections.keySet();
    /**
     * 循环每一个Node上的所有标签,看其是否在已知标签集合中
     */
    for (Entry<NodeId, Set<String>> entry : addedLabelsToNode.entrySet()) {
      if (!knownLabels.containsAll(entry.getValue())) {//说明该Node对应的标签有未知的,则抛异常
        String msg =
            "Not all labels being added contained by known "
                + "label collections, please check" + ", added labels=["
                + StringUtils.join(entry.getValue(), ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }
  
  /**
   * 为每一个节点添加对应的标签集合
   * 进入该方法,说明已经进行校验过了,label集合的内容都是合法的
   */
  @SuppressWarnings("unchecked")
  protected void internalAddLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode) throws IOException {
    // do add labels to nodes 新添加的节点和标签集合
    Map<NodeId, Set<String>> newNMToLabels = new HashMap<NodeId, Set<String>>();
        
    for (Entry<NodeId, Set<String>> entry : addedLabelsToNode.entrySet()) {
      NodeId nodeId = entry.getKey();//该node
      Set<String> labels = entry.getValue();//属于该node的标签集合
 
      createHostIfNonExisted(nodeId.getHost());//创建该host对象
      if (nodeId.getPort() == WILDCARD_PORT) {//为该host添加标签
        Host host = nodeCollections.get(nodeId.getHost());
        host.labels.addAll(labels);
        newNMToLabels.put(nodeId, host.labels);
      } else {
        createNodeIfNonExisted(nodeId);//为节点添加标签
        Node nm = getNMInNodeSet(nodeId);
        if (nm.labels == null) {
          nm.labels = new HashSet<String>();
        }
        nm.labels.addAll(labels);
        newNMToLabels.put(nodeId, nm.labels);
      }
    }

    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }

    // shows node->labels we added
    LOG.info("addLabelsToNode:");
    for (Entry<NodeId, Set<String>> entry : newNMToLabels.entrySet()) {
      LOG.info("  NM=" + entry.getKey() + ", labels=["
          + StringUtils.join(entry.getValue().iterator(), ",") + "]");
    }
  }
  
  /**
   * add more labels to nodes
   * 
   * @param addedLabelsToNode node -> labels map
   * 添加每一个节点添加对应的标签集合
   */
  public void addLabelsToNode(Map<NodeId, Set<String>> addedLabelsToNode)
      throws IOException {
    addedLabelsToNode = normalizeNodeIdToLabels(addedLabelsToNode);
    checkAddLabelsToNode(addedLabelsToNode);
    internalAddLabelsToNode(addedLabelsToNode);
  }
  
  /**
   * 检查,确保要删除的lable集合内的标签都是合法存在的标签
   */
  protected void checkRemoveFromClusterNodeLabels(Collection<String> labelsToRemove) throws IOException {
    if (null == labelsToRemove || labelsToRemove.isEmpty()) {
      return;
    }

    // Check if label to remove doesn't existed or null/empty, will throw
    // exception if any of labels to remove doesn't meet requirement
    for (String label : labelsToRemove) {
      label = normalizeLabel(label);
      if (label == null || label.isEmpty()) {
        throw new IOException("Label to be removed is null or empty");
      }
      
      if (!labelCollections.containsKey(label)) {
        throw new IOException("Node label=" + label
            + " to be removed doesn't existed in cluster "
            + "node labels collection.");
      }
    }
  }
  
  /**
   * 移除方法
   */
  @SuppressWarnings("unchecked")
  protected void internalRemoveFromClusterNodeLabels(Collection<String> labelsToRemove) {
    // remove labels from nodes 删除每一个节点中对应的标签
    for (String nodeName : nodeCollections.keySet()) {
      Host host = nodeCollections.get(nodeName);
      if (null != host) {
        host.labels.removeAll(labelsToRemove);
        for (Node nm : host.nms.values()) {
          if (nm.labels != null) {
            nm.labels.removeAll(labelsToRemove);
          }
        }
      }
    }

    /**
     * 在全局中删除标签
     */
    // remove labels from node labels collection
    for (String label : labelsToRemove) {
      labelCollections.remove(label);
    }

    // create event to remove labels记录删除标签的日志
    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(new RemoveClusterNodeLabels(labelsToRemove));
    }

    LOG.info("Remove labels: ["
        + StringUtils.join(labelsToRemove.iterator(), ",") + "]");
  }

  /**
   * Remove multiple node labels from repository
   * 
   * @param labelsToRemove
   *          node labels to remove
   * @throws IOException
   * 移除方法
   */
  public void removeFromClusterNodeLabels(Collection<String> labelsToRemove)
      throws IOException {
    labelsToRemove = normalizeLabels(labelsToRemove);
    
    checkRemoveFromClusterNodeLabels(labelsToRemove);

    internalRemoveFromClusterNodeLabels(labelsToRemove);
  }
  
  /**
   * 校验从指定node上移除指定label
   * 1.校验要移除的label标签必须都合法
   * 2.校验要移除的node是否存在
   * 3.该node对应的标签集合中必须都包含所有的要删除的属于该node的标签
   */
  protected void checkRemoveLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode) throws IOException {
    // check all labels being added existed 获取合法标签集合
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : removeLabelsFromNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      //标签必须合法
      if (!knownLabels.containsAll(labels)) {
        String msg =
            "Not all labels being removed contained by known "
                + "label collections, please check" + ", removed labels=["
                + StringUtils.join(labels, ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
      
      //暂存该node中的标签集合
      Set<String> originalLabels = null;
      
      //判断该node是否存在
      boolean nodeExisted = false;
      if (WILDCARD_PORT != nodeId.getPort()) {
        Node nm = getNMInNodeSet(nodeId);
        if (nm != null) {
          originalLabels = nm.labels;
          nodeExisted = true;//说明node存在
        }
      } else {
        Host host = nodeCollections.get(nodeId.getHost());
        if (null != host) {
          originalLabels = host.labels;
          nodeExisted = true;//说明node存在
        }
      }
      
      //node不存在,则抛异常
      if (!nodeExisted) {
        String msg =
            "Try to remove labels from NM=" + nodeId
                + ", but the NM doesn't existed";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // the labels will never be null
      if (labels.isEmpty()) {
        continue;
      }

      // originalLabels may be null,
      // because when a Node is created, Node.labels can be null.该node中的标签集合必须包含待删除的标签集合
      if (originalLabels == null || !originalLabels.containsAll(labels)) {
        String msg =
            "Try to remove labels = [" + StringUtils.join(labels, ",")
                + "], but not all labels contained by NM=" + nodeId;
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }
  
  /**
   * 移除每一个node对应的标签集合,进入该方法,说明已经校验过了
   */
  @SuppressWarnings("unchecked")
  protected void internalRemoveLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode) {
    // do remove labels from nodes
    Map<NodeId, Set<String>> newNMToLabels = new HashMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : removeLabelsFromNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();
      
      if (nodeId.getPort() == WILDCARD_PORT) {
        Host host = nodeCollections.get(nodeId.getHost());//从host中移除label
        host.labels.removeAll(labels);
        newNMToLabels.put(nodeId, host.labels);
      } else {
        Node nm = getNMInNodeSet(nodeId);//从指定node上移除label
        if (nm.labels != null) {
          nm.labels.removeAll(labels);
          newNMToLabels.put(nodeId, nm.labels);
        }
      }
    }
    
    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(
          new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }

    // shows node->labels we added
    LOG.info("removeLabelsFromNode:");
    for (Entry<NodeId, Set<String>> entry : newNMToLabels.entrySet()) {
      LOG.info("  NM=" + entry.getKey() + ", labels=["
          + StringUtils.join(entry.getValue().iterator(), ",") + "]");
    }
  }
  
  /**
   * remove labels from nodes, labels being removed most be contained by these
   * nodes
   * 移除每一个node对应的标签集合
   * @param removeLabelsFromNode node -> labels map
   */
  public void
      removeLabelsFromNode(Map<NodeId, Set<String>> removeLabelsFromNode)
          throws IOException {
    removeLabelsFromNode = normalizeNodeIdToLabels(removeLabelsFromNode);
    
    checkRemoveLabelsFromNode(removeLabelsFromNode);

    internalRemoveLabelsFromNode(removeLabelsFromNode);
  }
  
  /**
   * 检查待替换的标签是否正确,即是否在合法标签集合中
   */
  protected void checkReplaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode) throws IOException {
    if (null == replaceLabelsToNode || replaceLabelsToNode.isEmpty()) {
      return;
    }
    
    // check all labels being added existed
    Set<String> knownLabels = labelCollections.keySet();
    for (Entry<NodeId, Set<String>> entry : replaceLabelsToNode.entrySet()) {
      if (!knownLabels.containsAll(entry.getValue())) {
        String msg =
            "Not all labels being replaced contained by known "
                + "label collections, please check" + ", new labels=["
                + StringUtils.join(entry.getValue(), ",") + "]";
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }
  
  /**
   * 为每一个Node对应的标签做替换操作
   */
  @SuppressWarnings("unchecked")
  protected void internalReplaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode) throws IOException {
    // do replace labels to nodes
    Map<NodeId, Set<String>> newNMToLabels = new HashMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : replaceLabelsToNode.entrySet()) {
      NodeId nodeId = entry.getKey();
      Set<String> labels = entry.getValue();

      createHostIfNonExisted(nodeId.getHost());      
      if (nodeId.getPort() == WILDCARD_PORT) {//对host级别的标签全部替换成新的
        Host host = nodeCollections.get(nodeId.getHost());
        host.labels.clear();
        host.labels.addAll(labels);
        newNMToLabels.put(nodeId, host.labels);
      } else {//对node级别的标签全部替换成新的
        createNodeIfNonExisted(nodeId);
        Node nm = getNMInNodeSet(nodeId);
        if (nm.labels == null) {
          nm.labels = new HashSet<String>();
        }
        nm.labels.clear();
        nm.labels.addAll(labels);
        newNMToLabels.put(nodeId, nm.labels);
      }
    }

    if (null != dispatcher) {
      dispatcher.getEventHandler().handle(
          new UpdateNodeToLabelsMappingsEvent(newNMToLabels));
    }

    // shows node->labels we added
    LOG.info("setLabelsToNode:");
    for (Entry<NodeId, Set<String>> entry : newNMToLabels.entrySet()) {
      LOG.info("  NM=" + entry.getKey() + ", labels=["
          + StringUtils.join(entry.getValue().iterator(), ",") + "]");
    }
  }
  
  /**
   * replace labels to nodes
   * 为每一个Node对应的标签做替换操作
   * @param replaceLabelsToNode node -> labels map
   */
  public void replaceLabelsOnNode(Map<NodeId, Set<String>> replaceLabelsToNode)
      throws IOException {
    replaceLabelsToNode = normalizeNodeIdToLabels(replaceLabelsToNode);
    
    checkReplaceLabelsOnNode(replaceLabelsToNode);

    internalReplaceLabelsOnNode(replaceLabelsToNode);
  }

  /**
   * Get mapping of nodes to labels
   * 
   * @return nodes to labels map
   * 获取每一个节点对应的标签集合
   */
  public Map<NodeId, Set<String>> getNodeLabels() {
    try {
      readLock.lock();
      Map<NodeId, Set<String>> nodeToLabels = new HashMap<NodeId, Set<String>>();
      for (Entry<String, Host> entry : nodeCollections.entrySet()) {
        String hostName = entry.getKey();
        Host host = entry.getValue();
        for (NodeId nodeId : host.nms.keySet()) {
          Set<String> nodeLabels = getLabelsByNode(nodeId);
          if (nodeLabels == null || nodeLabels.isEmpty()) {
            continue;
          }
          nodeToLabels.put(nodeId, nodeLabels);
        }
        if (!host.labels.isEmpty()) {
          nodeToLabels
              .put(NodeId.newInstance(hostName, WILDCARD_PORT), host.labels);
        }
      }
      return Collections.unmodifiableMap(nodeToLabels);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get existing valid labels in repository
   * 
   * @return existing valid labels in repository
   * 获取集群中的所有标签集合
   */
  public Set<String> getClusterNodeLabels() {
    try {
      readLock.lock();
      Set<String> labels = new HashSet<String>(labelCollections.keySet());
      labels.remove(NO_LABEL);
      return Collections.unmodifiableSet(labels);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 校验标签,不符合标准的抛异常
   * 标签不允许大于最大字符，同时标签命名要符合正则表达式
   */
  private void checkAndThrowLabelName(String label) throws IOException {
    if (label == null || label.isEmpty() || label.length() > MAX_LABEL_LENGTH) {
      throw new IOException("label added is empty or exceeds "
          + MAX_LABEL_LENGTH + " character(s)");
    }
    label = label.trim();

    boolean match = LABEL_PATTERN.matcher(label).matches();

    if (!match) {
      throw new IOException("label name should only contains "
          + "{0-9, a-z, A-Z, -, _} and should not started with {-,_}"
          + ", now it is=" + label);
    }
  }

  protected String normalizeLabel(String label) {
    if (label != null) {
      return label.trim();
    }
    return NO_LABEL;
  }

  /**
   * 标准化标签
   */
  private Set<String> normalizeLabels(Collection<String> labels) {
    Set<String> newLabels = new HashSet<String>();
    for (String label : labels) {
      newLabels.add(normalizeLabel(label));
    }
    return newLabels;
  }
  
  /**
   * 检查node是否在集群中
   */
  protected Node getNMInNodeSet(NodeId nodeId) {
    return getNMInNodeSet(nodeId, nodeCollections);
  }
  
  protected Node getNMInNodeSet(NodeId nodeId, Map<String, Host> map) {
    return getNMInNodeSet(nodeId, map, false);
  }

  /**
   * 在map中检查是否存在NodeId,并且NodeId要满足是否运行参数
   */
  protected Node getNMInNodeSet(NodeId nodeId, Map<String, Host> map,boolean checkRunning) {
    Host host = map.get(nodeId.getHost());
    if (null == host) {
      return null;
    }
    Node nm = host.nms.get(nodeId);
    if (null == nm) {
      return null;
    }
    if (checkRunning) {
      return nm.running ? nm : null; 
    }
    return nm;
  }

  /**
   * 返回该node对应的标签集合
   */
  protected Set<String> getLabelsByNode(NodeId nodeId) {
    return getLabelsByNode(nodeId, nodeCollections);
  }
  
  /**
   * 返回该node对应的标签集合
   */
  protected Set<String> getLabelsByNode(NodeId nodeId, Map<String, Host> map) {
    Host host = map.get(nodeId.getHost());
    if (null == host) {
      return EMPTY_STRING_SET;
    }
    Node nm = host.nms.get(nodeId);
    if (null != nm && null != nm.labels) {
      return nm.labels;
    } else {
      return host.labels;
    }
  }
  
  /**
   * 为该节点对应的host添加一个Node对象
   */
  protected void createNodeIfNonExisted(NodeId nodeId) throws IOException {
    Host host = nodeCollections.get(nodeId.getHost());
    if (null == host) {
      throw new IOException("Should create host before creating node.");
    }
    Node nm = host.nms.get(nodeId);
    if (null == nm) {
      host.nms.put(nodeId, new Node());
    }
  }
  
  /**
   * 如果不存在则创建该host对象
   */
  protected void createHostIfNonExisted(String hostName) {
    Host host = nodeCollections.get(hostName);
    if (null == host) {
      host = new Host();
      nodeCollections.put(hostName, host);
    }
  }
  
  /**
   * 格式化,仅对label标签进行格式化
   */
  protected Map<NodeId, Set<String>> normalizeNodeIdToLabels(Map<NodeId, Set<String>> nodeIdToLabels) {
    Map<NodeId, Set<String>> newMap = new HashMap<NodeId, Set<String>>();
    for (Entry<NodeId, Set<String>> entry : nodeIdToLabels.entrySet()) {
      NodeId id = entry.getKey();
      Set<String> labels = entry.getValue();
      newMap.put(id, normalizeLabels(labels)); 
    }
    return newMap;
  }
}
