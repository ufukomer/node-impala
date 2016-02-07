const gulp = require('gulp');
const thrift = require('gulp-thrift');

const version = 'chd5-2.1_5.3.0/';

const basePaths = {
    src: 'src/',
    dest: 'lib/'
};

const paths = {
    js: {
        src: basePaths.src + 'impala.js',
        dest: basePaths.dest
    },
    thrift: {
        src: basePaths.src + version + '*.thrift',
        dest: basePaths.dest + version
    }
};

// 'gulp' command runs watch task as default
gulp.task('default', ['watch']);

gulp.task('watch', ['js'], () => {
    // Watch changes on impala.js
    gulp.watch(paths.js.src, ['js']);
});

// Generate thrift files
gulp.task('thrift', () => {
    gulp.src(paths.thrift.src)
        .pipe(thrift({
            gen: 'js:node'
        }))
        .pipe(gulp.dest(paths.thrift.dest));
});

// Copy impla.js from src/ to lib/
gulp.task('js', () => {
    gulp.src(paths.js.src)
        .pipe(gulp.dest(paths.js.dest));
});
