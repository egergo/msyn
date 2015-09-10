module.exports = function(grunt) {

	grunt.initConfig({
		pkg: grunt.file.readJSON('package.json'),
		uglify: {
			options: {
				banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyyy-mm-dd") %> */\n'
			},
			build: {
				src: 'src/<%= pkg.name %>.js',
				dest: 'build/<%= pkg.name %>.min.js'
			}
		},
		mochaTest: {
			test: {
				options: {
					reporter: process.env.TEST_REPORT_JUNIT === '1' ? 'mocha-junit-reporter' : 'spec',
					reporterOptions: {
						mochaFile: './build/test-results.xml'
					},
				},
				src: ['test/*.js'],
			},
		},

		exec: {
			cover: {
				cmd: './node_modules/.bin/istanbul cover grunt --dir ./build/coverage --print both -- test',
				stdout: false
			}
		},

		clean: {
			all: {
				src: ['build']
			}
		},

		jshint: {
			node: {
				src: ['*.js', 'test/**.js', 'app_data/**.js', 'items/**.js', 'platform_services/**.js', 'realms/**.js']
			}
		}
	});

	grunt.loadNpmTasks('grunt-mocha-test');
	grunt.loadNpmTasks('grunt-exec');
	grunt.loadNpmTasks('grunt-contrib-clean');
	grunt.loadNpmTasks('grunt-contrib-jshint');

	grunt.registerTask('default', ['test', 'cover']);
	grunt.registerTask('test', ['mochaTest']);
	grunt.registerTask('cover', ['exec:cover']);
};
