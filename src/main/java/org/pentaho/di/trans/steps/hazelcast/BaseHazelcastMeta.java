/*******************************************************************************
 *
 * Copyright (C) 2014-2015 by Matt Burgess
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.hazelcast;

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Created by mburgess on 3/23/15.
 */
public abstract class BaseHazelcastMeta extends BaseStepMeta implements StepMetaInterface {

  protected Set<InetSocketAddress> servers;

  public Set<InetSocketAddress> getServers() {
    return servers;
  }

  public void setServers( Set<InetSocketAddress> servers ) {
    this.servers = servers;
  }
}
