# Copyright 2015 Philipp Pahl, Sven Schubert
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

class Scheduler(object):
    """Scheduler handles task management.

    Scheduler:
    ** Events are defined, they are triggered eg. at a given time, time interval, job finish, manual trigger
    ** Events can have a payload for eg. information on path to file, Kafka topic
    ** Pipelines can subscribe and publish events
    ** use PubSub: http://pubsub.sourceforge.net/usage/usage_basic.html
    """

    @staticmethod
    def Instance():
        if not Scheduler._active_instance:
            Scheduler._active_instance = Scheduler()
        return Scheduler._active_instance

    _active_instance = None

    def __init__(self):
        if Scheduler._active_instance:
            raise ValueError(
                "Cannot run multiple Scheduler instances at once."
                "Use peachbox.Scheduler.Instance() to access existing instance.")
        Scheduler._active_instance = self
