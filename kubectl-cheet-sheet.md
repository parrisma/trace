https://www.elastic.co/guide/en/cloud-on-k8s/current/k8s-deploy-elasticsearch.html

Find Kubernetes master IP
<br>
<code>kubectl get nodes -o wide</code>

Increase mem limit for elastic as the WSL level

wsl -d docker-desktop
sysctl -w vm.max_map_count=262144

Delete Services

<code>kubectl delete --all services --namespace=[*here-you-enter-namespace*]</code>
<br>
<code>kubectl delete deployment deployment-name</code>


Set Secret to allow docker hub access.
<code>kubectl create secret docker-registry docker.io --docker-username=parrisma --docker-password=[*Docker Hub Access Key*] --docker-email=parris3142@hotmail.com</code>