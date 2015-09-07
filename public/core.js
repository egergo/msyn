
(function(window, undefined) {

var angular = window.angular;

function LoginManager($rootScope, $mdDialog) {
	this.$rootScope = $rootScope;
	this.$mdDialog = $mdDialog;
	this.isLoggedIn = false;
	this.isLoginInProgress = false;
	this.token;
	if (window.localStorage) {
		this.token = window.localStorage.token;
		if (this.token) {
			this.isLoggedIn = true;
		}
	}

	$rootScope.$on('message', this._handleMessage.bind(this));
}

LoginManager.prototype._handleMessage = function(e, messageEvent) {
	console.log('message', e, messageEvent);
	//this.$rootScope.$apply(function() {
	try {
		// TODO: check origin
		message = JSON.parse(messageEvent.data);
		if (message.type !== 'token') { return; }
		this.token = message.token;
		if (window.localStorage) {
			window.localStorage.token = message.token;
		}
		this.isLoggedIn = true;
		if (this.w) {
			this.w.close();
			window.focus();
		}
	} catch (err) {
		// ignore
	}
	//}.bind(this));
};

LoginManager.prototype.startLogin = function() {
	var url = window.location.protocol + '//' + window.location.host + '/auth/bnet';
	var w = window.open(url, 'bnetauth', 'width=640,height=700,menubar=0,toolbar=0,personalbar=0,directories=0,status=0,dependent=1,dialog=1');
	var interval = setInterval(function() {
		if (w.closed) {
			clearInterval(interval);
			console.log('closed');
		}
	});
	this.w = w;
};

LoginManager.prototype.logout = function() {
	var self = this;

	var dialog = this.$mdDialog.confirm()
		.title('Logout')
		.content('Do you really want to log out?')
		.ok('Yup')
		.cancel('Nope');
	this.$mdDialog.show(dialog).then(function() {
		self.token = undefined;
		self.isLoggedIn = false;
	});
};

angular.module('msyn', ['ngRoute', 'ngMaterial', 'ngResource', 'angularMoment'])

.directive('errSrc', function() {
  return {
    link: function(scope, element, attrs) {
      element.bind('error', function() {
        if (attrs.src != attrs.errSrc) {
          attrs.$set('src', attrs.errSrc);
        }
      });
    }
  }
})

.config(function($routeProvider) {
	$routeProvider
		.when('/', {
			templateUrl: 'characters.html',
			controller: 'CharactersCtrl'
		})
		.when('/auctions', {
			templateUrl: 'auctions.html',
			controller: 'AuctionsCtrl'
		})
		.when('/realmStatus', {
			templateUrl: 'realmStatus.html',
			controller: 'RealmStatusCtrl'
		})
		.when('/settings', {
			templateUrl: 'settings.html',
			controller: 'SettingsCtrl'
		})
		.otherwise({
			redirectTo: '/'
		});
})

.run(function($rootScope) {
	window.addEventListener('message', function(e) {
		$rootScope.$apply(function() {
			$rootScope.$broadcast('message', e);
		});
	});
})

// .run(function($rootScope, Token) {
// 	$rootScope.$on('message', function(e, e2) {
// 		var message = e2.data;
// 		try {
// 			var json = JSON.parse(message);
// 			if (json.type === 'token') {
// 				var token = json.token;
// 				Token.set(token);
// 			}
// 		} catch (err) {
// 			console.log(err);
// 		}
// 	});
// })

// .factory('Token', function($rootScope) {
// 	var result = {
// 		set: function(token) {
// 			console.log('updating token', token);
// 			this._token = token;
// 			$rootScope.$broadcast('tokenUpdated');
// 		},
// 		get: function() {
// 			console.log('getting token', this._token);
// 			return this._token;
// 		}
// 	};
// 	return result;
// })

.service('loginManager', LoginManager)

.factory('Characters', function($resource, loginManager) {
	return $resource('/characters', null, {
		get: {
			headers: {authorization: function() { return 'Bearer ' + loginManager.token; }}
		}
	});
})

.factory('Auctions', function($resource, loginManager) {
	return $resource('/auctions', null, {
		get: {
			headers: {authorization: function() { return 'Bearer ' + loginManager.token; }},
			isArray: true
		}
	});
})

.factory('RealmStatus', function($resource, loginManager) {
	return $resource('/realmStatus', null, {
		get: {
			headers: {authorization: function() { return 'Bearer ' + loginManager.token; }}
		}
	});
})

.factory('Settings', function($resource, loginManager) {
	return $resource('/settings', null, {
		get: {
			headers: {authorization: function() { return 'Bearer ' + loginManager.token; }}
		},
		save: {
			method: 'POST',
			headers: {authorization: function() { return 'Bearer ' + loginManager.token; }}
		}
	});
})

.controller('MainCtrl', function($scope, $timeout, $mdSidenav, loginManager) {
	var self = this;

	$scope.openMenu = openMenu;
	$scope.loginManager = loginManager;


  function openMenu() {
    $timeout(function() { $mdSidenav('left').open(); });
  }
})

.controller('CharactersCtrl', function($scope, Characters, $rootScope) {
	$rootScope.title = 'Characters';
	$scope.$watch('loginManager.isLoggedIn', function(value, oldValue) {
		$scope.characters = Characters.get();
	});
})

.controller('AuctionsCtrl', function($scope, Auctions, $rootScope) {
	$rootScope.title = 'Auctions';
	$scope.$watch('loginManager.isLoggedIn', function(value, oldValue) {
		$scope.auctions = Auctions.get();
	});
})

.controller('RealmStatusCtrl', function($scope, RealmStatus, $rootScope) {
	$rootScope.title = 'Realm Status';
	$scope.realmStatus = RealmStatus.get();
})

.controller('SettingsCtrl', function($scope, $rootScope, Settings, $q, $mdToast) {
	$rootScope.title = 'Settings';

	$scope.loaded = false;
	$scope.settings = Settings.get();
	$scope.settings.$promise.then(function() {
		$scope.loaded = true;
	}).catch(function(err) {
		console.error(err);
		$scope.error = 'Error ' + err.status + ': ' + err.data.substring(0, 100);
	});

	$scope.saving = false;
	$scope.save = function() {
		if ($scope.saving) { return; }

		$scope.saving = true;
		$q.when().then(function() {
			return $scope.settings.$save();
		}).then(function(res) {
			$mdToast.show($mdToast.simple().position('top left').content('Settings saved'));
		}).catch(function(err) {
			var msg = 'Error ' + err.status + ': ' + err.data.substring(0, 100);
			$mdToast.show($mdToast.simple().position('top left').content(msg));
			console.error(err);
		}).finally(function() {
			$scope.saving = false;
		});
	};

	$scope.sendTest = function() {

	};
})

;

})(window);
