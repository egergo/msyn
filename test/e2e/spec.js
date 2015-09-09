
describe('Navigation', function() {

	beforeEach(function() {
		browser.get('http://127.0.0.1:3000/');
	});

	it('should login', function() {

		return element(by.buttonText('Login')).click().then(function() {
			browser.sleep(500);
			return browser.getAllWindowHandles().then(function (handles) {
				console.log('handles', handles);
				newWindowHandle = handles[1]; // this is your new window
				return browser.switchTo().window(newWindowHandle).then(function() {
					return browser.driver.getCurrentUrl().then(function(url) {
						console.log(url);
					});
        });
      });
		})
	});

});

