/*
*
* Copyright 2013 Netflix, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package com.netflix.client;

import com.netflix.client.config.IClientConfig;

/**
 * There are multiple classes (and components) that need access to the configuration.
 * Its easier to do this by using {@link IClientConfig} as the object that carries these configurations
 * and to define a common interface that components that need this can implement and hence be aware of.
 *
 * @author stonse
 * @author awang 
 *
 */
public interface IClientConfigAware {

    /**
     * 工厂类
     */
    interface Factory {
        Object create(String type, IClientConfig config) throws InstantiationException, IllegalAccessException, ClassNotFoundException;
    }

    default void initWithNiwsConfig(IClientConfig clientConfig) {

    }

    /**
     * 使用IClientConfig进行初始化
     */
    default void initWithNiwsConfig(IClientConfig clientConfig, Factory factory) {
        initWithNiwsConfig(clientConfig);
    }
    
}
