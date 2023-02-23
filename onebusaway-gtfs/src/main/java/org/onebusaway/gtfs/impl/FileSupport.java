/**
 * Copyright (C) 2023 Cambridge Systematics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileSupport {

  private List<File> toDelete = new ArrayList<>();
  public FileSupport() {
  }

  public void markForDeletion(File file) {
    toDelete.add(file);
  }

  public void cleanup() {
    for (File file : toDelete) {
      deleteFileRecursively(file);
    }
    toDelete.clear();
  }

  public void deleteFileRecursively(File file) {
    if (file == null)
      return;

    if (!file.exists())
      return;

    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File child : files)
          deleteFileRecursively(child);
      }
    }

    file.delete();
  }

}
