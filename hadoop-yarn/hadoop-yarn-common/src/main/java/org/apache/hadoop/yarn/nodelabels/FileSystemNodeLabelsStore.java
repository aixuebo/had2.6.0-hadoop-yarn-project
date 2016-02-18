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

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.AddToClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.RemoveFromClusterNodeLabelsRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ReplaceLabelsOnNodeRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.AddToClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RemoveFromClusterNodeLabelsRequestPBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.ReplaceLabelsOnNodeRequestPBImpl;

import com.google.common.collect.Sets;

/**
 * 将标签信息存储到文件系统中
 */
public class FileSystemNodeLabelsStore extends NodeLabelsStore {

  public FileSystemNodeLabelsStore(CommonNodeLabelsManager mgr) {
    super(mgr);
  }

  protected static final Log LOG = LogFactory.getLog(FileSystemNodeLabelsStore.class);

  protected static final String DEFAULT_DIR_NAME = "node-labels";
  protected static final String MIRROR_FILENAME = "nodelabel.mirror";//已经保存过的镜像文件,先是存储ADD_LABELS,然后存储NODE_TO_LABELS
  protected static final String EDITLOG_FILENAME = "nodelabel.editlog";//正在编辑的日志信息
  
  protected enum SerializedLogType {
    ADD_LABELS,//添加标签集合
    NODE_TO_LABELS,//向一个节点添加对应的标签集合
    REMOVE_LABELS//删除标签集合
  }

  Path fsWorkingPath;//标签存储的目录
  FileSystem fs;
  FSDataOutputStream editlogOs;
  Path editLogPath;//相当于编辑日志
  
  //标签存储的默认目录
  private String getDefaultFSNodeLabelsRootDir() throws IOException {
    // default is in local: /tmp/hadoop-yarn-${user}/node-labels/
    return "file:///tmp/hadoop-yarn-"
        + UserGroupInformation.getCurrentUser().getShortUserName() + "/"
        + DEFAULT_DIR_NAME;
  }

  @Override
  public void init(Configuration conf) throws Exception {
    fsWorkingPath =
        new Path(conf.get(YarnConfiguration.FS_NODE_LABELS_STORE_ROOT_DIR,
            getDefaultFSNodeLabelsRootDir()));

    setFileSystem(conf);

    // mkdir of root dir path
    fs.mkdirs(fsWorkingPath);
  }

  @Override
  public void close() throws IOException {
    try {
      fs.close();
      editlogOs.close();
    } catch (IOException e) {
      LOG.warn("Exception happened whiling shutting down,", e);
    }
  }

  private void setFileSystem(Configuration conf) throws IOException {
    Configuration confCopy = new Configuration(conf);//本地复制了一份配置文件
    confCopy.setBoolean("dfs.client.retry.policy.enabled", true);
    String retryPolicy =
        confCopy.get(YarnConfiguration.FS_NODE_LABELS_STORE_RETRY_POLICY_SPEC,
            YarnConfiguration.DEFAULT_FS_NODE_LABELS_STORE_RETRY_POLICY_SPEC);
    confCopy.set("dfs.client.retry.policy.spec", retryPolicy);
    fs = fsWorkingPath.getFileSystem(confCopy);
    
    // if it's local file system, use RawLocalFileSystem instead of
    // LocalFileSystem, the latter one doesn't support append.
    if (fs.getScheme().equals("file")) {//获取本地文件系统对象
      fs = ((LocalFileSystem)fs).getRaw();
    }
  }
  
  private void ensureAppendEditlogFile() throws IOException {
    editlogOs = fs.append(editLogPath);//对该path文件进行追加操作
  }
  
  private void ensureCloseEditlogFile() throws IOException {
    editlogOs.close();
  }
  /**
   * 向日志中记录为每一个node映射上对应的标签
   */
  @Override
  public void updateNodeToLabelsMappings(
      Map<NodeId, Set<String>> nodeToLabels) throws IOException {
    ensureAppendEditlogFile();
    editlogOs.writeInt(SerializedLogType.NODE_TO_LABELS.ordinal());
    ((ReplaceLabelsOnNodeRequestPBImpl) ReplaceLabelsOnNodeRequest.newInstance(nodeToLabels)).getProto().writeDelimitedTo(editlogOs);
    ensureCloseEditlogFile();
  }

  /**
   * 向日志中记录要添加的标签
   */
  @Override
  public void storeNewClusterNodeLabels(Set<String> labels)
      throws IOException {
    ensureAppendEditlogFile();
    editlogOs.writeInt(SerializedLogType.ADD_LABELS.ordinal());
    ((AddToClusterNodeLabelsRequestPBImpl) AddToClusterNodeLabelsRequest.newInstance(labels)).getProto().writeDelimitedTo(editlogOs);
    ensureCloseEditlogFile();
  }

  /**
   * 向日志中记录要删除的标签集合
   */
  @Override
  public void removeClusterNodeLabels(Collection<String> labels)
      throws IOException {
    ensureAppendEditlogFile();
    editlogOs.writeInt(SerializedLogType.REMOVE_LABELS.ordinal());
    ((RemoveFromClusterNodeLabelsRequestPBImpl) RemoveFromClusterNodeLabelsRequest.newInstance(Sets
        .newHashSet(labels.iterator()))).getProto().writeDelimitedTo(editlogOs);
    ensureCloseEditlogFile();
  }

  @Override
  public void recover() throws IOException {
    /*
     * Steps of recover
     * 1) Read from last mirror (from mirror or mirror.old)
     * 2) Read from last edit log, and apply such edit log
     * 3) Write new mirror to mirror.writing
     * 4) Rename mirror to mirror.old
     * 5) Move mirror.writing to mirror
     * 6) Remove mirror.old
     * 7) Remove edit log and create a new empty edit log 
     */
    
    // Open mirror from serialized file
    Path mirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME);
    Path oldMirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME + ".old");

    FSDataInputStream is = null;
    if (fs.exists(mirrorPath)) {
      is = fs.open(mirrorPath);
    } else if (fs.exists(oldMirrorPath)) {
      is = fs.open(oldMirrorPath);
    }

    if (null != is) {
      Set<String> labels =
          new AddToClusterNodeLabelsRequestPBImpl(
              AddToClusterNodeLabelsRequestProto.parseDelimitedFrom(is)).getNodeLabels();
      Map<NodeId, Set<String>> nodeToLabels =
          new ReplaceLabelsOnNodeRequestPBImpl(
              ReplaceLabelsOnNodeRequestProto.parseDelimitedFrom(is))
              .getNodeToLabels();
      mgr.addToCluserNodeLabels(labels);
      mgr.replaceLabelsOnNode(nodeToLabels);
      is.close();
    }

    // Open and process editlog
    editLogPath = new Path(fsWorkingPath, EDITLOG_FILENAME);
    if (fs.exists(editLogPath)) {
      is = fs.open(editLogPath);

      while (true) {
        try {
          // read edit log one by one
          SerializedLogType type = SerializedLogType.values()[is.readInt()];
          
          switch (type) {
          case ADD_LABELS: {
            Collection<String> labels =
                AddToClusterNodeLabelsRequestProto.parseDelimitedFrom(is)
                    .getNodeLabelsList();
            mgr.addToCluserNodeLabels(Sets.newHashSet(labels.iterator()));
            break;
          }
          case REMOVE_LABELS: {
            Collection<String> labels =
                RemoveFromClusterNodeLabelsRequestProto.parseDelimitedFrom(is)
                    .getNodeLabelsList();
            mgr.removeFromClusterNodeLabels(labels);
            break;
          }
          case NODE_TO_LABELS: {
            Map<NodeId, Set<String>> map =
                new ReplaceLabelsOnNodeRequestPBImpl(
                    ReplaceLabelsOnNodeRequestProto.parseDelimitedFrom(is))
                    .getNodeToLabels();
            mgr.replaceLabelsOnNode(map);
            break;
          }
          }
        } catch (EOFException e) {
          // EOF hit, break
          break;
        }
      }
    }

    // Serialize current mirror to mirror.writing
    Path writingMirrorPath = new Path(fsWorkingPath, MIRROR_FILENAME + ".writing");
    FSDataOutputStream os = fs.create(writingMirrorPath, true);
    ((AddToClusterNodeLabelsRequestPBImpl) AddToClusterNodeLabelsRequestPBImpl
        .newInstance(mgr.getClusterNodeLabels())).getProto().writeDelimitedTo(os);
    ((ReplaceLabelsOnNodeRequestPBImpl) ReplaceLabelsOnNodeRequest
        .newInstance(mgr.getNodeLabels())).getProto().writeDelimitedTo(os);
    os.close();
    
    // Move mirror to mirror.old
    if (fs.exists(mirrorPath)) {
      fs.delete(oldMirrorPath, false);
      fs.rename(mirrorPath, oldMirrorPath);
    }
    
    // move mirror.writing to mirror
    fs.rename(writingMirrorPath, mirrorPath);
    fs.delete(writingMirrorPath, false);
    
    // remove mirror.old
    fs.delete(oldMirrorPath, false);
    
    // create a new editlog file
    editlogOs = fs.create(editLogPath, true);
    editlogOs.close();
    
    LOG.info("Finished write mirror at:" + mirrorPath.toString());
    LOG.info("Finished create editlog file at:" + editLogPath.toString());
  }
}
